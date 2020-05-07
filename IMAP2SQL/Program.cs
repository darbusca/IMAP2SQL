using System;
using System.Collections.Generic;
using System.Collections;
using System.Net.Sockets;
using System.Text;
using System.IO;
using System.Net.Mail;
namespace IMAP2SQL
{
    using S22.Imap;
    class Program
    {
        static void Main(string[] args)
        {
            string host = "10.10.0.3";
            int port = 7143;
            string username = "msg-retail@bricofer.it";
            string password = "Bricofer@25";
            //posta.bricofer.it 587 SSL
            //mail.smaail.com 587 SSL
            //BRICOFER/MSg-Retail + password
            //Msg-Retail@ottimax.it + password

            using (ImapClient Client = new ImapClient(host, port,
 username, password, AuthMethod.Login))
            {
                IEnumerable<uint> uids = Client.Search(SearchCondition.Unseen());
                // Download mail messages from the default mailbox.
                IEnumerable<MailMessage> messages = Client.GetMessages(uids);

                ClsDatabase odb = new ClsDatabase();
                odb.OpenConn();
                foreach (MailMessage mess in messages)
                {
                    string sql = "EXEC PR_INS_MAIL 'SELF','@FromAddress','@DisplayName','@Subject','@Body'";
                    sql = sql.Replace("@FromAddress", mess.From.Address);
                    sql = sql.Replace("@DisplayName", mess.From.DisplayName);
                    sql = sql.Replace("@Subject", mess.Subject);
                    sql = sql.Replace("@Body", mess.Body);


                    odb.ExecNoQuery(sql);
                }
                odb.CloseConn();   
            }



        }
    }
}
