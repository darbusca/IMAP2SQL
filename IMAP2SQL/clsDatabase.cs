using System;
using System.Data;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Security;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualBasic;
using System.Data.SqlClient;


namespace IMAP2SQL
{
    public class ClsDatabase
    {
        private const int def_ConnectionTimeOut = 120;
        private const int def_CommandTimeOut = 120;
        public string vbCrlf = System.Environment.NewLine;
        private bool _ThrowErr = false;
        public bool ThrowErr
        {
            get => _ThrowErr;
  
            set
            {
                _ThrowErr = value;
            }
        }

        private int _ConnectionTimeOut = 300;
        public int ConnectionTimeOut
        {
            get => _ConnectionTimeOut;

            set
            {
                if (value <= 0)
                    value = def_ConnectionTimeOut;
                _ConnectionTimeOut = value;
            }
        }

        private int _CommandTimeOut = def_CommandTimeOut;
        public int CommandTimeOut
        {
            get => _CommandTimeOut;

            set
            {
                if (value <= 0)
                    value = def_CommandTimeOut;
                _CommandTimeOut = value;
            }
        }

        private string _Server = "SQLHAGROUP01";
        /// <summary>
        ///     ''' Server
        ///     ''' </summary>
        public string Server
        {
            get => _Server;

            set
            {
                _Server = value;
            }
        }

        private string _User = "retail";
        /// <summary>
        ///     ''' Nome utente
        ///     ''' </summary>
        public string User
        {
            get => _User;

            set
            {
                _User = value;
            }
        }

        private bool _isTrusted;
        /// <summary>
        ///     ''' Connessione Trusted
        ///     ''' </summary>
        public bool isTrusted
        {
            get => _isTrusted;

            set
            {
                _isTrusted = isTrusted;
            }
        }

        private string _Password = "Br1cofer";
        /// <summary>
        ///     ''' Password
        ///     ''' </summary>
        public string Password
        {
            get => _Password;

            set
            {
                _Password = value;
            }
        }

        private string _Catalog = "Retail";
        /// <summary>
        ///     ''' Catalog
        ///     ''' </summary>
        public string Catalog
        {
            get => _Catalog;

            set
            {
                _Catalog = value;
            }
        }

        private SqlConnection _Con;

        /// <summary>
        ///     ''' Oggetto SqlConnection 
        ///     ''' </summary>
        public SqlConnection Conn
        {
            get => _Con;

            set
            {
                _Con = value;
                SplitConnectionProperty();
            }
        }

        private void SplitConnectionProperty()
        {
            bool bRet = false;
            try
            {
                string[] str = _Con.ConnectionString.Split(';');
                foreach (string item in str)
                {
                    // item = item.ToLower
                    switch (true)
                    {
                        case object _ when true == item.Contains("Server"):
                            {
                                _Server = item.Substring(item.IndexOf("=") + 1);
                                break;
                            }

                        case object _ when true == item.Contains("uid"):
                            {
                                _User = item.Substring(item.IndexOf("=") + 1);
                                break;
                            }

                        case object _ when true == item.Contains("pwd"):
                            {
                                _Password = item.Substring(item.IndexOf("=") + 1);
                                break;
                            }

                        case object _ when true == item.Contains("database"):
                            {
                                _Catalog = item.Substring(item.IndexOf("=") + 1);
                                break;
                            }
                    }
                }
            }
            catch (Exception ex)
            {
            }
        }

        /// <summary>
        ///     ''' Metodo per aprire la connessione al database.
        ///     ''' </summary>
        public void OpenConn(bool _ReadOnly = false)
        {
            _ReadOnly = false;
            string sStrConn = "";
            string Sql = "";
            DataTable Dt = new DataTable();
            DataSet Ds = new DataSet();
            SqlConnection _Con1;
            SqlDataAdapter myDataAdapter = new SqlDataAdapter();

            Sql = Sql + " If SERVERPROPERTY('IsHadrEnabled') = 1                                                                        ";
            Sql = Sql + "   BEGIN                                                                                                       ";
            Sql = Sql + "           Select                                                                                              ";
            Sql = Sql + "                AGC.name                                                                                       ";
            Sql = Sql + "               ,RCS.replica_server_name	                                                                    ";
            Sql = Sql + "               ,ARS.role_desc                                                                                  ";
            Sql = Sql + "               ,AGL.dns_name				                                                                    ";
            Sql = Sql + "           FROM                                                                                                ";
            Sql = Sql + "           sys.availability_groups_cluster AS AGC                                                              ";
            Sql = Sql + "           INNER Join sys.dm_hadr_availability_replica_cluster_states AS RCS ON RCS.group_id = AGC.group_id    ";
            Sql = Sql + "           INNER Join sys.dm_hadr_availability_replica_states AS ARS ON ARS.replica_id = RCS.replica_id        ";
            Sql = Sql + "           INNER Join sys.availability_group_listeners AS AGL ON AGL.group_id = ARS.group_id                   ";
            Sql = Sql + "           Where  ARS.role_desc ='SECONDARY'                                                                   ";
            Sql = Sql + "   End	                                                                                                        ";
            // Sql = Sql & " Else                                                                                                          "
            // Sql = Sql & "   BEGIN                                                                                                       "
            // Sql = Sql & "           Select                                                                                              "
            // Sql = Sql & "       	 'SQLHAGROUP01'	As [name]					                                                        "
            // Sql = Sql & "           ,'SQLHAGROUP01' As replica_server_name		                                                        "
            // Sql = Sql & "           ,'PRIMARY'		As role_desc			                                                            "
            // Sql = Sql & "           ,'SQLHAGROUP01'	As dns_name				                                                            "
            // Sql = Sql & "   End"

            try
            {
                if (!(_Con == null) && _Con.State == ConnectionState.Open)
                    _Con.Close();

                if (_ReadOnly == true)
                {
                    sStrConn = "SQLHAGROUP01;uid=Retail;pwd=Br1cofer;database=Retail;Connection Timeout = 30;Application Name=IMAP2SQL";
                    _Con1 = new SqlConnection(sStrConn);
                    _Con1.Open();
                    myDataAdapter = new SqlDataAdapter(Sql, _Con1);
                    myDataAdapter.SelectCommand.CommandTimeout = CommandTimeOut;
                    myDataAdapter.Fill(Ds);
                    if (Ds.Tables.Count > 0)
                    {
                        if (Ds.Tables[0].Rows.Count > 0)
                            Server = Ds.Tables[0].Rows[0]["replica_server_name"].ToString();
                    }
                    _Con1.Close();
                }
                else
                {
                    sStrConn = "SQLHAGROUP01;uid=Retail;pwd=Br1cofer;database=Retail;Connection Timeout = 30;Application Name=IMAP2SQL";
                    _Con1 = new SqlConnection(sStrConn);
                    _Con1.Open();
                    myDataAdapter = new SqlDataAdapter(Sql, _Con1);
                    myDataAdapter.SelectCommand.CommandTimeout = CommandTimeOut;
                    myDataAdapter.Fill(Ds);
                    if (Ds.Tables.Count > 0)
                    {
                        if (Ds.Tables[0].Rows.Count > 0)
                            Server = Ds.Tables[0].Rows[0]["name"].ToString();
                    }
                    _Con1.Close();
                }

                sStrConn = "SQLHAGROUP01;uid=Retail;pwd=Br1cofer;database=Retail;Connection Timeout = 30;Application Name=IMAP2SQL";
                _Con = new SqlConnection(sStrConn);
                _Con.Open();
                _isConnect = true;
            }
            catch (Exception ex)
            {
                Debug.Print(ex.Message + vbCrlf + sStrConn);
                _isConnect = false;
                if (_ThrowErr)
                    throw;
            }
        }

        /// <summary>
        ///     ''' Metodo per chiudere la connessione al database.
        ///     ''' </summary>
        public void CloseConn()
        {
            try
            {
                if (!(_Con == null))
                {
                    if (_Con.State != ConnectionState.Closed)
                    {
                        _Con.Close();
                        _Con.Dispose();
                    }
                }
            }
            catch (Exception ex)
            {
            }
            _isConnect = false;
        }

        private bool _isConnect;

        /// <summary>
        ///     ''' Stato della connessione al database. Restituisce True se la connessione è aperta.
        ///     ''' </summary>
        public bool isConnect
        {
            get => _isConnect = true;
        }


        /// <summary>
        ///     ''' Metodo per estrazione dati SQL in DataSet
        ///     ''' </summary>
        ///     ''' <param name="sQuerySQL">Query da eseguire</param>
        ///     ''' <returns>DataSet</returns>
        public DataSet QueryPassTrough(string sQuerySQL)
        {
            SqlDataAdapter myDataAdapter;
            DataSet myDataSet = new DataSet();
            try
            {
                myDataAdapter = new SqlDataAdapter(sQuerySQL, _Con);
                myDataAdapter.SelectCommand.CommandTimeout = CommandTimeOut;

                // gestione dell'errore relativo alla connessione DBNETLib
                bool bInConnectionResume = false;
                try
                {
                    myDataAdapter.Fill(myDataSet);
                }
                catch (SqlException ex_sql)
                {
                    bInConnectionResume = true;
                }
                catch (InvalidOperationException ex_connection)
                {
                    if (ex_connection.ToString().ToLower().Contains("connection") | ex_connection.ToString().ToLower().Contains("connessione"))
                        bInConnectionResume = true;
                }
                finally
                {
                    if (bInConnectionResume == true)
                    {
                        System.Threading.Thread.Sleep(500);
                        this.OpenConn();
                        myDataAdapter = new SqlDataAdapter(sQuerySQL, _Con);
                        myDataAdapter.SelectCommand.CommandTimeout = CommandTimeOut;
                        myDataAdapter.Fill(myDataSet);
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.Print(ex.Message + vbCrlf + sQuerySQL);
                if (_ThrowErr)
                    throw;
            }

            return myDataSet;
        }


        public DataSet QueryPassTrough(string sQuerySQL, SqlParameter[] spParams)
        {
            SqlDataAdapter myDataAdapter;
            DataSet myDataSet = new DataSet();
            SqlCommand cmd = new SqlCommand(sQuerySQL, _Con);

            cmd.CommandTimeout = CommandTimeOut;
            if (!(spParams == null))
            {
                // AndAlso spParams.GetUpperBound(0) > 0 Then
                if (cmd.Parameters.Count > 0)
                    cmd.Parameters.Clear();
                foreach (SqlParameter Param in spParams)
                {
                    if (!(Param == null))
                        cmd.Parameters.Add(Param);
                }
            }



            myDataAdapter = new SqlDataAdapter(sQuerySQL, _Con);

            myDataAdapter.SelectCommand.CommandTimeout = CommandTimeOut;
            myDataAdapter.SelectCommand = cmd;

            bool bInConnectionResume = false;

            myDataAdapter.Fill(myDataSet);

            return myDataSet;
        }

        /// <summary>
        ///     ''' Ritorna il valore della prima riga, della prima colonna della query passata
        ///     ''' ritorna come oggetto non tipizzato, valorizzando il paramentro opzionale bRetValIsNull in caso di valore NULL
        ///     ''' </summary>
        ///     ''' <param name="sQuerySQL">Query da eseguire</param>
        ///     ''' <param name="bRetValIsNull"></param>
        ///     ''' <returns></returns>
        public object ExecScalar(string sQuerySQL, SqlParameter[] spParams, ref bool bRetValIsNull )
        {
            object objRet = null;
            bRetValIsNull = false;
            try
            {
                SqlCommand cmd = new SqlCommand(sQuerySQL, _Con);
                cmd.CommandTimeout = CommandTimeOut;

                if (!(spParams == null))
                {
                    // AndAlso spParams.GetUpperBound(0) > 0 Then
                    foreach (SqlParameter Param in spParams)
                    {
                        if (!(Param == null))
                            cmd.Parameters.Add(Param);
                    }
                }

                // gestione dell'errore relativo alla connessione DBNETLib
                bool bInConnectionResume = false;
                try
                {
                    objRet = cmd.ExecuteScalar();
                }
                catch (SqlException ex_sql)
                {
                    bInConnectionResume = true;
                }
                catch (InvalidOperationException ex_connection)
                {
                    if (ex_connection.ToString().ToLower().Contains("connection") | ex_connection.ToString().ToLower().Contains("connessione"))
                        bInConnectionResume = true;
                }
                finally
                {
                    if (bInConnectionResume == true)
                    {
                        // inserire un wait 
                        System.Threading.Thread.Sleep(500);
                        this.OpenConn();
                        cmd = new SqlCommand(sQuerySQL, _Con);
                        cmd.CommandTimeout = CommandTimeOut;
                        objRet = cmd.ExecuteScalar();
                    }
                }

                if (objRet == null || objRet == DBNull.Value)
                {
                    bRetValIsNull = true;
                    objRet = DBNull.Value;
                }
            }
            catch (Exception ex)
            {
                Debug.Print(ex.Message + vbCrlf + sQuerySQL);
                if (_ThrowErr)
                    throw;
            }

            return objRet;
        }

        /// <summary>
        ///     ''' Ritorna il valore della prima riga, della prima colonna della query passata
        ///     ''' ritorna come oggetto non tipizzato, valorizzando il paramentro opzionale bRetValIsNull in caso di valore NULL
        ///     ''' </summary>
        ///     ''' <param name="sQuerySQL">Query da eseguire</param>
        ///     ''' <param name="bRetValIsNull"></param>
        ///     ''' <returns></returns>
        public object ExecScalar(string sQuerySQL, ref bool bRetValIsNull)
        {
            object objRet = null;
            bRetValIsNull = false;
            try
            {
                SqlCommand cmd = new SqlCommand(sQuerySQL, _Con);
                cmd.CommandTimeout = CommandTimeOut;

                // gestione dell'errore relativo alla connessione DBNETLib
                bool bInConnectionResume = false;
                try
                {
                    objRet = cmd.ExecuteScalar();
                }
                catch (SqlException ex_sql)
                {
                    bInConnectionResume = true;
                }
                catch (InvalidOperationException ex_connection)
                {
                    if (ex_connection.ToString().ToLower().Contains("connection") | ex_connection.ToString().ToLower().Contains("connessione"))
                        bInConnectionResume = true;
                }
                finally
                {
                    if (bInConnectionResume == true)
                    {
                        // inserire un wait 
                        System.Threading.Thread.Sleep(500);
                        this.OpenConn();
                        cmd = new SqlCommand(sQuerySQL, _Con);
                        cmd.CommandTimeout = CommandTimeOut;
                        objRet = cmd.ExecuteScalar();
                    }
                }

                if (objRet == null || objRet == DBNull.Value)
                {
                    bRetValIsNull = true;
                    objRet = DBNull.Value;
                }
            }
            catch (Exception ex)
            {
                Debug.Print(ex.Message + vbCrlf + sQuerySQL);
                if (_ThrowErr)
                    throw;
            }

            return objRet;
        }


        /// <summary>
        ///     ''' Esegue un comando SQL senza ritorno di valori
        ///     ''' </summary>
        ///     ''' <param name="sQuerySQL">Query da eseguire</param>
        ///     ''' <returns></returns>
        public bool ExecNoQuery(string sQuerySQL)
        {
            bool bRet = false;
            try
            {
                SqlCommand cmd = new SqlCommand(sQuerySQL, _Con);
                cmd.CommandTimeout = CommandTimeOut;

                // gestione dell'errore relativo alla connessione DBNETLib
                bool bInConnectionResume = false;
                try
                {
                    cmd.ExecuteNonQuery();
                    bRet = true;
                }
                catch (SqlException ex_sql)
                {
                    bInConnectionResume = true;
                }
                catch (InvalidOperationException ex_connection)
                {
                    if (ex_connection.ToString().ToLower().Contains("connection") | ex_connection.ToString().ToLower().Contains("connessione"))
                        bInConnectionResume = true;
                }
                finally
                {
                    if (bInConnectionResume == true)
                    {
                        System.Threading.Thread.Sleep(500);
                        this.OpenConn();
                        cmd = new SqlCommand(sQuerySQL, _Con);
                        cmd.CommandTimeout = CommandTimeOut;
                        cmd.ExecuteNonQuery();
                        bRet = true;
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.Print(ex.Message + vbCrlf + sQuerySQL);
                bRet = false;
                if (_ThrowErr)
                    throw;
            }

            return bRet;
        }


        public bool ExecNoQuery(string sQuerySQL, SqlParameter spParam)
        {
            bool bRet = false;
            bool bInConnectionResume = false;

            try
            {
                // gestione dell'errore relativo alla connessione DBNETLib
                SqlCommand cmd = new SqlCommand(sQuerySQL, _Con);
                cmd.CommandTimeout = CommandTimeOut;

                try
                {
                    cmd.Parameters.Clear();
                    cmd.Parameters.Add(spParam);

                    cmd.ExecuteNonQuery();

                    bRet = true;
                }
                catch (SqlException ex_sql)
                {
                    bInConnectionResume = true;
                }
                catch (InvalidOperationException ex_connection)
                {
                    if (ex_connection.ToString().ToLower().Contains("connection") | ex_connection.ToString().ToLower().Contains("connessione"))
                        bInConnectionResume = true;
                }
                finally
                {
                    if (bInConnectionResume == true)
                    {
                        System.Threading.Thread.Sleep(500);
                        this.OpenConn();
                        cmd = new SqlCommand(sQuerySQL, _Con);
                        cmd.CommandTimeout = CommandTimeOut;
                        cmd.ExecuteNonQuery();
                        bRet = true;
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.Print(ex.Message + vbCrlf + sQuerySQL);
                bRet = false;
                if (_ThrowErr)
                    throw;
            }

            return bRet;
        }

        public bool ExecNoQuery(string sQuerySQL, int a, SqlParameter[] spParam)
        {
            bool bRet = false;
            try
            {
                // gestione dell'errore relativo alla connessione DBNETLib
                SqlCommand cmd = new SqlCommand(sQuerySQL, _Con);
                cmd.CommandTimeout = CommandTimeOut;
                cmd.Parameters.Clear();
                bool bInConnectionResume = false;
                try
                {
                    foreach (SqlParameter Param in spParam)
                        cmd.Parameters.Add(Param);

                    cmd.ExecuteNonQuery();

                    bRet = true;
                }
                catch (SqlException ex_sql)
                {
                    bInConnectionResume = true;
                }
                catch (InvalidOperationException ex_connection)
                {
                    if (ex_connection.ToString().ToLower().Contains("connection") | ex_connection.ToString().ToLower().Contains("connessione"))
                        bInConnectionResume = true;
                }
                finally
                {
                    if (bInConnectionResume == true)
                    {
                        System.Threading.Thread.Sleep(500);
                        this.OpenConn();
                        cmd = new SqlCommand(sQuerySQL, _Con);
                        cmd.CommandTimeout = CommandTimeOut;
                        cmd.ExecuteNonQuery();
                        bRet = true;
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.Print(ex.Message + vbCrlf + sQuerySQL);
                bRet = false;
                if (_ThrowErr)
                    throw;
            }

            return bRet;
        }
    }
}
