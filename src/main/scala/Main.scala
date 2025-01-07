import jobs.Aggregations.{BatteryLevelStatsJob, DeviceConnectivityAggJob, DevicesByLocationAggJob, DevicesByTypeAggJob, UserActiveDevicesAggJob}
import jobs.StreamingAnonymizationJob
import producer.MockDataWriter
import utils.HadoopConfig.Hadoop.address

object Main {
  def main(args: Array[String]): Unit = {

    val inputPath = s"hdfs://$address/user/ovidiu/processed/anonymized"
    val outputPathJob1 = s"hdfs://$address/user/ovidiu/processed/anonymized"
    val outputPathJob2 = s"hdfs://$address/user/ovidiu/processed/battery-level"
    val outputPathJob3 = s"hdfs://$address/user/ovidiu/processed/device-connectivity"
    val outputPathJob4 = s"hdfs://$address/user/ovidiu/processed/devices-by-location"
    val outputPathJob5 = s"hdfs://$address/user/ovidiu/processed/devices-by-type"
    val outputPathJob6 = s"hdfs://$address/user/ovidiu/processed/user-active-devices"
    val checkpointPathJob1 = s"hdfs:///$address/user/ovidiu/checkpoints/anonymization-job"


    //    StreamingAnonymizatioJob if active spark/kafka cluster
    //    new StreamingAnonymizationJob(outputPathJob1, checkpointPathJob1).run()

    new MockDataWriter(outputPathJob1).run()

    new BatteryLevelStatsJob(inputPath, outputPathJob2).run()

    new DeviceConnectivityAggJob(inputPath, outputPathJob3).run()

    new DevicesByLocationAggJob(inputPath, outputPathJob4).run()

    new DevicesByTypeAggJob(inputPath, outputPathJob5).run()

    new UserActiveDevicesAggJob(inputPath, outputPathJob6).run()
  }
}