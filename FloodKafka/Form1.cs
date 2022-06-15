using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace FloodKafka
{
    public partial class Form1 : Form
    {
        int sentCount = 0;
        bool isRunning = true;

        ProducerConfig producerConfig;
        Thread loopShootThread;

        CancellationToken cancellationToken1, cancellationToken2, cancellationToken3;


        public Form1()
        {
            InitializeComponent();
        }

        private void label3_Click(object sender, EventArgs e)
        {

        }

        private void textBox3_TextChanged(object sender, EventArgs e)
        {

        }

        private void Form1_Load(object sender, EventArgs e)
        {
            producerConfig = new ProducerConfig
            {
                BootstrapServers = textBox5.Text,
                ClientId = "giang-flood-kafka"
            };

        }

        private void button1_Click(object sender, EventArgs e)
        {
            sentCount = 0;
            button1.Enabled = false;
            loopShootThread = new Thread(new ThreadStart(SendMessages));
            loopShootThread.Start();
        }

        public void SendMessages()
        {
            try
            {
                SendMessage();
            }
            catch { }
        }

        public void SendMessage()
        {
            int shouldSendCount = Int32.Parse(textBox3.Text);
            try
            {
                using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
                {
                    string message = "";
                    string topic = "";
                    int userId = 1;
                    bool userIdIncrese = false;

                    textBox1.Invoke((MethodInvoker)(() => topic = textBox1.Text));
                    richTextBox1.Invoke((MethodInvoker)(() => message = richTextBox1.Text));
                    textBox2.Invoke((MethodInvoker)(() => userId = Int32.Parse(textBox2.Text)));
                    checkBox1.Invoke((MethodInvoker)(() => userIdIncrese = checkBox1.Checked));

                    for (int i = 0; i < shouldSendCount; i++)
                    {
                        int calcUserId = userId;
                        if (userIdIncrese)
                        {
                            calcUserId = userId + i;
                        }
                        producer.Produce(topic, new Message<string, string> { Key = calcUserId.ToString(), Value = message.Replace("$userId$", calcUserId.ToString()) });
                        
                        sentCount++;

                        if (sentCount == shouldSendCount)
                        {
                            label7.BeginInvoke(new Action(() => label7.Text = "Đã bắn " + sentCount + " lần. XONG"));

                            button1.BeginInvoke(new Action(() => button1.Enabled = true));
                            loopShootThread.Abort();

                        }
                        else
                        {
                            label7.BeginInvoke(new Action(() => label7.Text = "Đã bắn " + sentCount + " lần"));
                        }

                        Thread.Sleep(Int32.Parse(textBox4.Text));
                    }
                    producer.Flush();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }
    }
}
