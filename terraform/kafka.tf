provider "kafka" {
  bootstrap_servers = ["127.0.0.1:9092"]
  skip_tls_verify = true
  tls_enabled = false
}

resource "kafka_topic" "logs" {
  name               = "logs"
  // Every topic partition in Kafka is replicated n times, where n is the replication factor of the topic.
  // This allows Kafka to automatically failover to these replicas when a server in the cluster fails
  // so that messages remain available in the presence of failures
  replication_factor = 2
  // The number of partitions the topic should have
  partitions         = 100

  config = {
    "segment.ms"     = "20000"
    "cleanup.policy" = "compact"
  }
}