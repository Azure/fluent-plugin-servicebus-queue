
require 'fluent/test'
require 'fluent/test/driver/output'
require 'fluent/test/helpers'
require 'fluent/plugin/out_azure_servicebus_queue'

include Fluent::Test::Helpers

class AzureServicebusQueueTest < Test::Unit::TestCase
    def setup
        Fluent::Test.setup
    end

    CONFIG = %q!
        type azure_servicebus_queue
        namespace test_namespace
        queueName test_queue_name
        accessKeyName send
        accessKeyValueFile /etc/password/queuePassword
        format json
        timeToLive 60
    !

    MSICONFIG = %q!
    type azure_servicebus_queue
    namespace test_namespace
    queueName test_queue_name
    format json
    timeToLive 60
    use_msi true
    client_id abc
!

    def create_driver(conf)
        Fluent::Test::Driver::Output.new(Fluent::Plugin::AzureServicebusQueue) do
          # for testing.    
          def write(chunk)
            @emit_streams = []
            event = chunk.read
            @emit_streams << event
          end

          private
        
        end.configure(conf)
    end

    def test_configure
        d = create_driver(CONFIG)
        assert_equal 'test_namespace', d.instance.namespace
        assert_equal 'test_queue_name', d.instance.queueName
        assert_equal 'send', d.instance.accessKeyName
        assert_equal '/etc/password/queuePassword', d.instance.accessKeyValueFile
        assert_equal 60, d.instance.timeToLive
        assert_equal 'message', d.instance.field
    end


    def test_msi_configure
        d = create_driver(MSICONFIG)
        assert_equal 'test_namespace', d.instance.namespace
        assert_equal 'test_queue_name', d.instance.queueName
        assert_equal 60, d.instance.timeToLive
        assert_equal 'message', d.instance.field
        assert_equal true, d.instance.use_msi
        assert_equal 'abc', d.instance.client_id
    end
end