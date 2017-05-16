require 'kubeclient'
require 'trollop'
require 'kafka'

logger = Logger.new(STDOUT)
kafka = Kafka.new(
  seed_brokers: ["localhost:9092"],
  logger: logger,
)

def parse_args(argv)
  opts = Trollop.options do
    banner <<-EOS
Collect openshift inventory and write to kafka

Usage:
  collector.rb [OPTION]
EOS
    opt :host, 'IP address or hostname of your openshift cluster', :type => :string
    opt :port, 'Port of your openshift api', :type => :string, :default => '8443'
    opt :token, 'API Token', :type => :string
  end
end

def kubernetes_connect(hostname, port, options = {})
  endpoint = "https://#{hostname}:#{port}/api"
  version = "v1"

  ssl_options = Kubeclient::Client::DEFAULT_SSL_OPTIONS.merge(
    :verify_ssl => OpenSSL::SSL::VERIFY_NONE
  )

  Kubeclient::Client.new(
    endpoint,
    version,
    :ssl_options  => ssl_options,
    :auth_options => {
      :bearer_token => options[:token]
    }
  )
end

def verify_credentials(conn)
  return conn.api_valid?
end

def connect(host, port, token)
  conn = kubernetes_connect(host, port, :token => token)
  raise unless verify_credentials(conn)

  conn
end

def stream_inventory(stream, message)
  stream.produce(YAML::dump(message), topic: 'inventory')
end

def parse_pod(pod)
  {
    :ems_ref => pod.metadata.uid,
    :name    => pod.metadata.name
  }
end

def parse_container(container)
  {
    :ems_ref => container.name,
    :name    => container.name,
  }
end

def refresh(kubeconn, stream)
  kubeconn.get_pods.each do |pod|
    result = parse_pod(pod)
    puts result[:name]

    stream_inventory(stream, result)
    pod.spec.containers.each do |container|
      stream_inventory(stream, parse_container(container))
    end
  end
end

opts = parse_args(ARGV)

conn = connect(opts[:host], opts[:port], opts[:token])

puts "Connected to OpenShift"

producer = kafka.async_producer
producer.deliver_messages

while true do
  puts "Refreshing provider"
  refresh(conn, producer)
  puts "Refreshing provider...Complete"
  sleep 10
end
