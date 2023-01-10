using System;
using System.Collections.Generic;
using System.Text;
using SolaceSystems.Solclient.Messaging;
using System.Threading;

namespace MessagingClient
{
    /// <summary>
    /// Demonstrates how to use Solace Systems Messaging API for sending and receiving a guaranteed delivery message
    /// </summary>
    public class QueueConsumer : IDisposable
    {
        private IContext? context;
        private ISession? session;
        private IQueue? queue;
        private IFlow? flow;
        private EventWaitHandle WaitEventWaitHandle = new AutoResetEvent(false);
        
        private readonly object messagesLock = new object(); 
        private List<byte[]> BinaryMessages = new List<byte[]>();
        
        private const int DefaultReconnectRetries = 3;
        
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

        public void subscribe(string queueName)
        {
            if (session == null)
            {
                throw new InvalidOperationException("Session not yet created, use createConnection(...)");
            }

            // Create the queue
            queue = ContextFactory.Instance.CreateQueue(queueName);

            // Create and start flow to the provisioned queue
            // NOTICE HandleMessageEvent as the message event handler 
            // and HandleFlowEvent as the flow event handler
            Console.WriteLine("Waiting for a message in the queue '{0}'...", queueName);
            flow = session.CreateFlow(new FlowProperties()
                {
                    AckMode = MessageAckMode.ClientAck
                },
                queue, null, HandleMessageEvent, HandleFlowEvent);
        }

        public void startListening()
        {
            if (flow == null)
            {
                throw new InvalidOperationException("Flow not yet created, use subscribe(queueName)");
            }
            flow.Start();
        }

        public void closeConnection()
        {
            ContextFactory.Instance.Cleanup();
        }


        /// <summary>
        /// This event handler is invoked by Solace Systems Messaging API when a message arrives
        /// </summary>
        /// <param name="source"></param>
        /// <param name="args"></param>
        private void HandleMessageEvent(object source, MessageEventArgs args)
        {
            // Received a message
            // Console.WriteLine("Wrapper Logs: Received message.");
            using (IMessage message = args.Message)
            {
                // Expecting the message content as a binary attachment
                lock (messagesLock)  
                {  
                    BinaryMessages.Add(message.BinaryAttachment);
                }
                
                Console.WriteLine("Wrapper Consumed! {0}", Encoding.ASCII.GetString(message.BinaryAttachment));
                // ACK the message
                if (flow == null)
                {
                    throw new InvalidOperationException("Flow not yet created, use subscribe(queueName), then start using startListening()");
                }
                flow.Ack(message.ADMessageId);
                // finish the program
                WaitEventWaitHandle.Set();
            }
        }

        public void HandleFlowEvent(object sender, FlowEventArgs args)
        {
            // Received a flow event
            Console.WriteLine("Received Flow Event '{0}' Type: '{1}' Text: '{2}'",
                args.Event,
                args.ResponseCode.ToString(),
                args.Info);
        }

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
                        session = null;
                    }
                    if (queue != null)
                    {
                        queue.Dispose();
                        queue = null;
                    }
                    if (flow != null)
                    {
                        flow.Dispose();
                        flow = null;
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

        #region Main
        private void consumeMessage()
        {
            Console.Write("Wrapper Logs: Waiting...");
            WaitEventWaitHandle.WaitOne();
        }
        
        public byte[][] Consume()
        {
            try
            {
                consumeMessage();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception thrown: {0}", ex.Message);
            }

            byte[][] messageArray;
            lock (messagesLock)  
            {  
                messageArray = BinaryMessages.ToArray();
                BinaryMessages.Clear();
            }  
            return messageArray;
        }
        #endregion
    }
}