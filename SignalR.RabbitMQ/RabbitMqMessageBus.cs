using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace SignalR.RabbitMQ
{
    public class RabbitMqMessageBus : IMessageBus, IIdGenerator<long>, IDisposable
    {
        private readonly TimeSpan _rabbitConnectionRetryPeriod = TimeSpan.FromMilliseconds(200);
        private readonly InProcessMessageBus<long> _bus;
        private readonly string _rabbitmqExchangeName;
        private readonly ConnectionFactory _connectionFactory;

        private readonly Object _modelAccessSerializationLock = new Object(); //Models can only be accessed one thread at a time.
        private IModel _model; //We cache the model.

        private int _count;

        private Boolean _isDisposed = false;

        public RabbitMqMessageBus(IDependencyResolver resolver, string rabbitMqExchangeName, ConnectionFactory connectionFactory)
        {
            _bus = new InProcessMessageBus<long>(resolver, this);
            _rabbitmqExchangeName = rabbitMqExchangeName;
            _connectionFactory = connectionFactory;

            ListenForMessages();
        }

        public Task<MessageResult> GetMessages(IEnumerable<string> eventKeys, string id, CancellationToken timeoutToken)
        {
            return _bus.GetMessages(eventKeys, id, timeoutToken);
        }

        public Task Send(string connectionId, string eventKey, object value)
        {
            var message = new RabbitMqMessageWrapper(connectionId, eventKey, value);
            return Task.Factory.StartNew(SendMessage, message);
        }

        public long ConvertFromString(string value)
        {
            return Int64.Parse(value, CultureInfo.InvariantCulture);
        }

        public string ConvertToString(long value)
        {
            return value.ToString(CultureInfo.InvariantCulture);
        }

        public long GetNext()
        {
            return _count++;
        }

        private void SendMessage(object state)
        {
            Boolean sent = false;
            while (GetIsAlive() && !sent)
            {
                try
                {
                    IModel model = GetExistingOrCreateNewModel();

                    var message = (RabbitMqMessageWrapper)state;
                    byte[] payload = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(message));

                    //The model can be accessed from different threads, but NOT overlapped.
                    lock (_modelAccessSerializationLock)
                    {
                        model.BasicPublish(_rabbitmqExchangeName, message.EventKey, null, payload);
                    }

                    sent = true;
                }
                catch (IOException)
                {
                    /*SWALLOW*/
                    DestroyModel();
                    Thread.Sleep(_rabbitConnectionRetryPeriod);
                }
                catch (OperationInterruptedException)
                {
                    /*SWALLOW*/
                    DestroyModel();
                    Thread.Sleep(_rabbitConnectionRetryPeriod);
                }
            } 
        }

        private IModel GetExistingOrCreateNewModel()
        {
            lock (_modelAccessSerializationLock)
            {
                IModel model = _model;

                if (model == null)
                {
                    global::RabbitMQ.Client.IConnection connection = _connectionFactory.CreateConnection();
                    model = connection.CreateModel();
                    connection.AutoClose = true;

                    _model = model;

                    //Ensure the Exchange is declared.
                    model.ExchangeDeclare(_rabbitmqExchangeName, "topic", true);                    
                }

                return model;
            }
        }

        private Boolean GetIsAlive()
        {
            return !_isDisposed && !System.Environment.HasShutdownStarted;
        }

        /// <summary>
        /// Doesn't throw
        /// </summary>
        private void DestroyModel()
        {
            IModel model;

            lock (_modelAccessSerializationLock)
            {
                model = _model;
                _model = null;
            }

            if (model != null)
            {
                try
                {
                    model.Close();
                    model.Dispose();
                }
                catch {/*SWALLOW*/}
            }
        }

        private void ListenForMessages()
        {            
            ThreadPool.QueueUserWorkItem(_ =>
                {
                    while (GetIsAlive())
                    {
                        try
                        {
                            QueueingBasicConsumer consumer;

                            IModel model = GetExistingOrCreateNewModel();

                            lock (_modelAccessSerializationLock)
                            {
                                var queue = model.QueueDeclare("", false, false, true, null);

                                model.QueueBind(queue.QueueName, _rabbitmqExchangeName, "#");

                                consumer = new QueueingBasicConsumer(model);
                                model.BasicConsume(queue.QueueName, false, consumer);
                            }

                            while (GetIsAlive())
                            {
                                BasicDeliverEventArgs ea = null;
                                Object dequeuedMessage;

                                //Peep at "GetIsAlive()" every 500 ms to support cancelling on Dispose.
                                Boolean messageReceived = consumer.Queue.Dequeue(500, out dequeuedMessage);
                                if (messageReceived)
                                {
                                    ea = (BasicDeliverEventArgs)dequeuedMessage;
                                }
                                else
                                {
                                    //We timed out waiting for the message
                                    continue;
                                }

                                lock (_modelAccessSerializationLock)
                                {
                                    model.BasicAck(ea.DeliveryTag, false);
                                }

                                string json = Encoding.UTF8.GetString(ea.Body);

                                var message = JsonConvert.DeserializeObject<RabbitMqMessageWrapper>(json);
                                _bus.Send(message.ConnectionIdentifier, message.EventKey, message.Value);
                            }
                        }
                        catch (IOException)
                        {
                            /*SWALLOW*/
                            DestroyModel();
                            Thread.Sleep(_rabbitConnectionRetryPeriod);
                        }
                        catch (OperationInterruptedException)
                        {
                            /*SWALLOW*/
                            DestroyModel();
                            Thread.Sleep(_rabbitConnectionRetryPeriod);
                        }
                    }
                });
        }

        public void Dispose()
        {
            if (_isDisposed)
            {
                return;
            }
            _isDisposed = true;

            DestroyModel();
        }
    }
}