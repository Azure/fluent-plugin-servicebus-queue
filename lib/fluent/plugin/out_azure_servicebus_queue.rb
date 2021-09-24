require 'fluent/plugin/output'
require 'cgi'
require 'openssl'
require 'base64'
require 'net/http'
require 'faraday'

module Fluent::Plugin
  class AzureServicebusQueue < Output
    Fluent::Plugin.register_output("azure_servicebus_queue", self)

    config_param :namespace, :string
    config_param :queueName, :string
    config_param :accessKeyName, :string, :default => nil
    config_param :accessKeyValueFile, :string, :default => nil
    config_param :timeToLive, :integer
    config_param :field, :string, :default => "message"
    config_param :useMSI, :bool, default: false
    config_param :clientIDFile, :string, :default => nil

    # method for sync buffered output mode
    def write(chunk)
      read = chunk.read()
      split = read.split("\n")

      url = "https://#{namespace}.servicebus.windows.net/#{queueName}/messages"

      if useMSI
        client_id = getCientID
        token = generateMSIToken(client_id)
      else 
        keyValue = getAccessKeyValue
        token = generateToken(url, accessKeyName, keyValue)
      end

      uri = URI.parse(url)
      https = Net::HTTP.new(uri.host, uri.port)
      https.use_ssl = true
      https.verify_mode = OpenSSL::SSL::VERIFY_NONE
      request = Net::HTTP::Post.new(uri.request_uri)
      request['Content-Type'] = 'application/json'
      request['Authorization'] = token
      request['BrokerProperties'] = "{\"Label\":\"fluentd\",\"State\":\"Active\",\"TimeToLive\":#{timeToLive}}"

      chunk.each do |time, record|
        request.body = record[field]
        response  = https.request(request)
      end
    end

    def getAccessKeyValue
      File.read(accessKeyValueFile).strip
    end

    def getCientID
      File.read(clientIDFile).strip
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

    # reference1: https://github.com/microsoft/fluent-plugin-azure-storage-append-blob/blob/master/lib/fluent/plugin/out_azure-storage-append-blob.rb
    # reference2: https://github.com/Azure/azure-sdk-for-ruby/blob/master/runtime/ms_rest_azure/lib/ms_rest_azure/credentials/msi_token_provider.rb
    # reference3: https://docs.microsoft.com/en-us/rest/api/servicebus/send-message-to-queue
    def generateMSIToken(clientid)
      access_key_request = Faraday.new('http://169.254.169.254/metadata/identity/oauth2/token?' \
                                      "api-version=2018-02-01" \
                                      '&resource=https://servicebus.azure.net/' \
                                      "&client_id=#{clientid}",
                                      headers: { 'Metadata' => 'true' })
                                  .get
                                  .body
      access_token = JSON.parse(access_key_request)['access_token']

      "Bearer #{access_token}"
    end
  end
end