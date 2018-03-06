require 'fluent/plugin/output'
require 'cgi'
require 'openssl'
require 'base64'
require 'net/http'

module Fluent::Plugin
  class AzureServicebusQueue < Output
    Fluent::Plugin.register_output("azure_servicebus_queue", self)

    config_param :namespace, :string
    config_param :queueName, :string
    config_param :accessKeyName, :string
    config_param :accessKeyValueFile, :string
    config_param :timeToLive, :integer

    # method for sync buffered output mode
    def write(chunk)
      read = chunk.read()
      split = read.split("\n")

      url = "https://#{namespace}.servicebus.windows.net/#{queueName}/messages"
      keyValue = getAccessKeyValue
      token = generateToken(url, accessKeyName, keyValue)

      uri = URI.parse(url)
      https = Net::HTTP.new(uri.host, uri.port)
      https.use_ssl = true
      https.verify_mode = OpenSSL::SSL::VERIFY_NONE
      request = Net::HTTP::Post.new(uri.request_uri)
      request['Content-Type'] = 'application/json'
      request['Authorization'] = token
      request['BrokerProperties'] = "{\"Label\":\"fluentd\",\"State\":\"Active\",\"TimeToLive\":#{timeToLive}}"

      chunk.each do |time, record|
        request.body = record["message"]
        https.request(request)
      end
    end

    def getAccessKeyValue
      File.read(accessKeyValueFile).strip
    end

    def generateToken(url,key_name,key_value)
      target_uri = CGI.escape(url.downcase).gsub('+', '%20').downcase
      expires = Time.now.to_i + 10
      to_sign = "#{target_uri}\n#{expires}"

      signature = CGI.escape(
          Base64.strict_encode64(
            OpenSSL::HMAC.digest(
              OpenSSL::Digest.new('sha256'), key_value, to_sign
            )
          )
        ).gsub('+', '%20')

      "SharedAccessSignature sr=#{target_uri}&sig=#{signature}&se=#{expires}&skn=#{key_name}"
    end
  end
end