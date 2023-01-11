using System;
using System.Text;
using SolaceSystems.Solclient.Messaging;

namespace MessagingClient {
    
    public class Producer
    {
        #region Class Variables
        private IContext? context;
        private ISession? session;
        private const int DefaultReconnectRetries = 3;
        #endregion
        
        public void createConnection(string host, string vpnName, string userName, string passWord)
        {
            // Initialize Solace Systems Messaging API with logging to console at Warning level
            ContextFactoryProperties cfp = new ContextFactoryProperties()
            {
                SolClientLogLevel = SolLogLevel.Warning
            };
            cfp.LogToConsoleError();
            ContextFactory.Instance.Init(cfp);
            
            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException("Solace Messaging Router host name must be non-empty.", "host");
            }
            if (string.IsNullOrWhiteSpace(vpnName))
            {
                throw new InvalidOperationException("VPN name must be non-empty.");
            }
            if (string.IsNullOrWhiteSpace(userName))
            {
                throw new InvalidOperationException("Client username must be non-empty.");
            }
            
            SessionProperties sessionProps = new SessionProperties()
            {
                Host = host,
                VPNName = vpnName,
                UserName = userName,
                Password = passWord,
                ReconnectRetries = DefaultReconnectRetries,
                // TODO: Validate SSL Certificate
                // https://solace.community/discussion/512/c-net-integration-getting-error-failed-to-create-session
                SSLValidateCertificate = false
            };

            context = ContextFactory.Instance.CreateContext(new ContextProperties(), null);
            if (context == null)
            {
                throw new ArgumentException("Solace Systems API context Router must be not null.", "context");
            }

            session = context.CreateSession(sessionProps, null, null);
            if (session == null)
            {
                throw new ArgumentException("Solace Systems API session must be not null.", "session");
            } 

            Console.WriteLine("Wrapper Logs: Connecting as {0}@{1} on {2}...", userName, vpnName, host);
            ReturnCode returnCode = session.Connect();
            if (returnCode != ReturnCode.SOLCLIENT_OK)
            {
                Console.WriteLine("Wrapper Logs: Error connecting, return code: {0}", returnCode);
                throw new InvalidOperationException("Error connecting.");
                
            }
            
            Console.WriteLine("Wrapper Logs: Session successfully connected.");
        }

        public void closeConnection()
        {
            ContextFactory.Instance.Cleanup();
        }
        
        public void Produce(string topic, string message)
        {
            try
            {
                PublishMessage(topic, message);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Wrapper Logs: Exception thrown: {0}", ex.Message);
            }
        }

        #region Publish Message
        private void PublishMessage(string topic, string messageContent)
        {
            // Create the message
            using (IMessage message = ContextFactory.Instance.CreateMessage())
            {
                message.Destination = ContextFactory.Instance.CreateTopic(topic);
                // Create the message content as a binary attachment
                message.BinaryAttachment = Encoding.ASCII.GetBytes(messageContent);
                message.DMQEligible = true;

                // Publish the message to the topic on the Solace messaging router
                Console.WriteLine("Wrapper Logs: Publishing message... {0}...", Encoding.ASCII.GetString(message.BinaryAttachment));
                if (session == null)
                {
                    throw new ArgumentException("Solace Systems API session must be not null.", "session");
                } 
                ReturnCode returnCode = session.Send(message);
                
                if (returnCode != ReturnCode.SOLCLIENT_OK)
                {
                    Console.WriteLine("Wrapper Logs: Publishing failed, return code: {0}", returnCode);
                }
                
                Console.WriteLine("Wrapper Logs: Published!");
            }
        }
        #endregion
    }
}

