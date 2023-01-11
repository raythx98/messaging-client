using System;
using System.Collections.Generic;
using System.Text;
using SolaceSystems.Solclient.Messaging;
using System.Threading;

namespace MessagingClient {
    public class Consumer : IDisposable
    {
        #region Class Variables
        private IContext? context;
        private ISession? session;
        private EventWaitHandle WaitEventWaitHandle = new AutoResetEvent(false);
        
        private readonly object messagesLock = new object(); 
        private List<byte[]> BinaryMessages = new List<byte[]>();
        
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

            session = context.CreateSession(sessionProps, HandleMessage, null);
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

        public void subscribe(string topic)
        {
            // This is the topic on Solace messaging router where a message is published
            // Must subscribe to it to receive messages
            // TODO: check what if topic does not exist
            if (session == null)
            {
                throw new ArgumentException("Solace Systems API session must be not null.", "session");
            } 
            session.Subscribe(ContextFactory.Instance.CreateTopic(topic), true);
            Console.WriteLine("Wrapper Logs: Topic successfully subscribed... {0}", topic);
        }

        public void closeConnection()
        {
            ContextFactory.Instance.Cleanup();
        }
        
        public byte[][] Consume()
        {
            try
            {
                consumeMessage();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Wrapper Logs: Exception thrown: {0}", ex.Message);
            }

            byte[][] messageArray;
            lock (messagesLock)  
            {  
                messageArray = BinaryMessages.ToArray();
                BinaryMessages.Clear();
            }  
            return messageArray;
        }

        #region Handle Message
        /// <summary>
        /// This event handler is invoked by Solace Systems Messaging API when a message arrives
        /// </summary>
        /// <param name="source"></param>
        /// <param name="args"></param>
        private void HandleMessage(object source, MessageEventArgs args)
        {
            // Console.WriteLine("Wrapper Logs: Received published message.");
            // Received a message
            using (IMessage message = args.Message)
            {
                // Expecting the message content as a binary attachment
                lock (messagesLock)  
                {  
                    BinaryMessages.Add(message.BinaryAttachment);
                }
                
                Console.WriteLine("Wrapper Logs: Wrapper Consumed! {0}", Encoding.ASCII.GetString(message.BinaryAttachment));
                // finish the program
                WaitEventWaitHandle.Set();
            }
        }
        #endregion

        #region IDisposable Support
        private bool disposedValue = false;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    if (session != null)
                    {
                        session.Dispose();
                    }
                }
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
        
        #region Consume Message
        private void consumeMessage()
        {
            Console.WriteLine("Wrapper Logs: Waiting...");
            WaitEventWaitHandle.WaitOne();
        }
        #endregion
    }
}

