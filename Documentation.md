# Technical Overview: Data Anonymization in the Streaming Pipeline

This document provides a **technical perspective** on how we implement data anonymization in our real-time data ingestion and processing pipeline. While these techniques can be applied in various industrial contexts, this overview is addressed particularly to an academic audience—such as university professors—who may be interested in the precise mechanics of anonymization, pseudonymization, and data masking in large-scale distributed systems.

---

## 1. Motivation and Scope

1. **Research-Grade Privacy Preservation**  
   In an academic or R&D setting, data sharing and collaborative analysis are frequent. Anonymization enables us to retain analytical utility of the dataset while removing directly identifying or sensitive attributes, thus fostering secure, ethically responsible, and regulation-compliant research.

2. **Regulatory Compliance**  
   Legal frameworks (GDPR, CCPA, HIPAA, etc.) mandate specific controls over personal data. Our anonymization pipeline is designed to align with these standards, ensuring that once data enters our systems, it is processed and stored in a manner that protects individual privacy.

3. **Risk Reduction**  
   Even in university-driven research, data breaches or unauthorized access can have severe repercussions. By proactively anonymizing data, we minimize the risk of identity disclosure or re-identification attacks, safeguarding both participants and institutions.

---

## 2. Data Flow and Architecture

1. **Stream Ingestion**
    - **Source**: Data arrives from Kafka, file-based streams, or other micro-batch sources.
    - **Structure**: Raw messages typically contain fields such as \`id\`, \`userId\`, \`location\`, \`timestamp\`, and sensor readings.

2. **Parsing & Transformation**
    - **Schema & Validation**: Incoming bytes (Avro, JSON, or CSV) are parsed into a Spark Dataset/DataFrame, enabling column-based manipulations.
    - **Enrichment**: If required, additional context or metadata can be merged in (e.g., sensor models, user profiles) before anonymization.

3. **Anonymization Layer**
    - **Column-Based**: We define transformations on specific columns (e.g., `id`, `userId`, `location`) that might contain personally identifiable information (PII).
    - **Spark Transformations**: By leveraging Spark’s `.withColumn(...)` and UDFs, we methodically apply hashing, masking, noise addition, coarsening, and redacting.

4. **Storage & Consumption**
    - **Persistent Storage**: The anonymized DataFrame is stored in Parquet/Delta on HDFS (or a cloud equivalent).
    - **Downstream Use**: Researchers and analysts query these anonymized records for further processing (machine learning, statistical analysis, etc.).

---

## 3. Anonymization Techniques and Their Functions

Below are the main anonymization methods we employ, along with their **technical purposes**:

1. **Hashing (Pseudonymization)**
    - **Mechanism**: A cryptographic hash function (e.g., SHA-256) is applied to strings such as IDs, serial numbers, or user identifiers.
    - **Role**:
        - *Pseudonymization*: The original value is replaced by a deterministic but irreversible hash output.
        - *Utility Retention*: Entities can still be tracked for analytics (matching the same hash), without revealing the original identity.
    - **Example**:
      ```scala
      .withColumn("id", sha2(col("id"), 256))
      ```
      This ensures consistent but unrecoverable mappings for each `id`.

2. **String Masking (Partial Redaction)**
    - **Mechanism**: A custom function (UDF) that either replaces or obfuscates sensitive substrings. For instance, addresses might retain only city-level information or just the last few characters.
    - **Role**:
        - *De-Identification*: Removes direct references to names, streets, or coordinates while preserving partial context.
        - *Human-Readable Output*: Maintains some textual form for debugging or classification tasks.
    - **Example**:
      ```scala
      .withColumn("location", maskStringUdf(col("location")))
      ```
      where `maskStringUdf` may replace a portion of the address with `"REDACTED"` or remove the street number.

3. **Timestamp Coarsening**
    - **Mechanism**: Rounds or truncates timestamps to a coarser granularity (e.g., hourly, daily).
    - **Role**:
        - *Temporal Anonymity*: Prevents precise timeline reconstruction of a subject’s activities.
        - *Aggregate Analysis Preservation*: Summaries of usage over hours/days are still possible.
    - **Example**:
      ```scala
      .withColumn("lastUpdated", coarsenTimestampUdf(col("lastUpdated")))
      ```
      typically rounding `epochMillis` to the nearest hour.

4. **Noise Injection**
    - **Mechanism**: Adds small random perturbations (e.g., ± 0.5) to numeric columns such as sensor readings (temperature, CO2 levels).
    - **Role**:
        - *Privacy Enhancement*: Deters re-identification by micro-changes in numerical values.
        - *Statistical Utility*: Distributions remain close to the original, enabling valid analysis or modeling.
    - **Example**:
      ```scala
      .withColumn("co2Level", addNoiseDoubleUdf(col("co2Level")))
      ```
      ensuring the transformed value is not precisely traceable.

5. **Redacting Columns**
    - **Mechanism**: Substitutes entire string columns with `"REDACTED"` or null.
    - **Role**:
        - *Hard Removal of PII*: Useful if certain fields are deemed unnecessary for analytics or too high-risk to keep.
    - **Example**:
      ```scala
      .withColumn("manufacturer", lit("REDACTED"))
      ```

---

## 4. Technical Considerations

1. **Data Schema Evolution**
    - Because anonymization logic is often schema-dependent (e.g., referencing `AirQuality` vs. `Humidity` subfields), changes in the incoming data format might require adjustments in the anonymization pipeline.

2. **Performance Overheads**
    - Cryptographic hashes (SHA-256) and repeated UDF calls can introduce computational overhead. We typically batch or pipeline transformations to remain within real-time or micro-batch SLAs.

3. **Statistical Impact**
    - Adding noise or coarsening timestamps can bias certain types of statistical analyses. Calibration (e.g., noise magnitude) should be chosen to balance privacy and data utility.

4. **Union or Nested Field Handling**
    - In more complex schemas (e.g., Avro unions with multiple sensor sub-models), we use conditional logic to apply transformations only to relevant subfields, preserving others if they are null.

---

## 5. Example Anonymized Pipeline (Snippet)

Below is a simplified Spark snippet demonstrating how various transformations are combined:

```scala
val anonymizedDf = sensorDf
  .withColumn("id", sha2(col("id"), 256))
  .withColumn("location", maskStringUdf(col("location")))
  .withColumn("lastUpdated", coarsenTimestampUdf(col("lastUpdated")))
  .withColumn("unitModel", struct(
    when(col("unitModel.AirQuality").isNotNull,
      // apply random noise
      struct(
        addNoiseDoubleUdf(col("unitModel.AirQuality.co2Level")).alias("co2Level"),
        addNoiseDoubleUdf(col("unitModel.AirQuality.vocLevel")).alias("vocLevel"),
        col("unitModel.AirQuality.unit").alias("unit")
      )
    ).otherwise(col("unitModel.AirQuality")).alias("AirQuality"),

    // other subfields like Humidity, Motion, etc.
  ))

anonymizedDf.write.mode("overwrite").parquet("/path/to/hdfs/anonymized")
