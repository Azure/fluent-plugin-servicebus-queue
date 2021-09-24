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
   
    IMDS_TOKEN_ACQUIRE_URL = "http://169.254.169.254/metadata/identity/oauth2/token" # The managed identities for Azure resources endpoint for the Instance Metadata Service.
    API_VERSION = "2018-02-01" # the API version for the IMDS endpoint. Please use API version 2018-02-01 or greater.

    # method for sync buffered output mode
    def write(chunk)
      read = chunk.read()
      split = read.split("\n")

      url = "https://#{namespace}.servicebus.windows.net/#{queueName}/messages"

      if useMSI
        client_id = getClientID
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
        https.request(request)
      end
    end

    def getAccessKeyValue
      File.read(accessKeyValueFile).strip
    end

    def getClientID
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

    # reference1: https://docs.microsoft.com/en-us/rest/api/servicebus/send-message-to-queue (Instruction to send message to a service bus queue using Azure AD JWT token)
    # reference2: https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/how-to-use-vm-token (Instruction to use managed identities for\ 
    # Azure resources on an Azure VM to acquire an access token)
    def generateMSIToken(clientid)
      access_key_request = Faraday.new(IMDS_TOKEN_ACQUIRE_URL \
                                      "?api-version=#{API_VERSION}" \
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