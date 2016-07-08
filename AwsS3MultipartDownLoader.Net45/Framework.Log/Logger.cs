using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;

namespace Framework.Log
{

    //! TraceSource Class
    //! TextWriterTraceListener textWriterTraceListener = new TextWriterTraceListener("log_tracesource.txt", "LogFile");
    //! textWriterTraceListener.TraceOutputOptions = TraceOptions.DateTime | TraceOptions.ProcessId | TraceOptions.ThreadId;
    //! TraceSource traceSource = new TraceSource("TraceSource Log", SourceLevels.All);
    //! traceSource.Listeners.Add(textWriterTraceListener);
    //! traceSource.TraceEvent(TraceEventType.Error, 0, "hogehoge");
    //! traceSource.Flush();

    //!
    //! Logger.AddListener(new TextWriterTraceListener("log_file.txt"));
    //! Logger.AddListener(new ConsoleTraceListener());
    //!
    public class Logger
    {
        static List<TraceListener> _listeners = new List<TraceListener>();

        static bool _flushOnWrite = true;
        public static bool FlushOnWrite
        {
            get
            {
                return _flushOnWrite;
            }
            set 
            {
                _flushOnWrite = value; 
            }
        }

        static bool _dateTimeWrite = false;
        public static bool DateTimeWrite
        {
            get
            {
                return _dateTimeWrite;
            }
            set
            {
                _dateTimeWrite = value;
            }
        }

        static bool _enabled = true;
        public static bool Enabled
        {
            get
            {
                return _enabled;
            }
            set
            {
                _enabled = value;
            }
        }


        public static void AddListener(TraceListener traceListener)
        {
            lock (_listeners)
            {
                _listeners.Add(traceListener);
            }
        }

        public static void AddListener(string path)
        {
            try
            {
                if (string.IsNullOrEmpty(path))
                    return;

                string directoryPath = Path.GetDirectoryName(path);
                if (!string.IsNullOrEmpty(directoryPath) && !Directory.Exists(directoryPath))
                    Directory.CreateDirectory(directoryPath);

                lock (_listeners)
                {
                    FileStream fileStream = new FileStream(path, FileMode.Append, FileAccess.Write);
                    _listeners.Add(new TextWriterTraceListener(fileStream));
                }
            }
            catch
            {
            }
        }

        public static void AddConsoleTraceListener()
        {
            lock (_listeners)
            {
                _listeners.Add(new ConsoleTraceListener());
            }
        }

        public static void RemoveListener(TraceListener traceListener)
        {
            lock (_listeners)
            {
                traceListener.Close();
                _listeners.Remove(traceListener);
            }
        }

        public static void ClearListener()
        {
            lock (_listeners)
            {
                foreach (TraceListener t in _listeners)
                {
                    t.Close();
                }

                _listeners.Clear();
            }
        }

        public static void Write(string message)
        {
            if (_enabled)
            {
                lock (_listeners)
                {
                    TraceListener[] traceListeners = _listeners.ToArray();
                    string text = _dateTimeWrite ? string.Format("{0} [{1}] {2}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss:fff"), Thread.CurrentThread.ManagedThreadId, message) : message;
                    foreach (TraceListener t in traceListeners)
                    {
                        t.Write(text);
                        if (_flushOnWrite)
                            t.Flush();
                    }
                }
            }
        }

        public static void Write(string format, params object[] args)
        {
            Write(string.Format(format, args));
        }

        public static void WriteLine()
        {
            Write(Environment.NewLine);
        }

        public static void WriteLine(string message)
        {
            Write(message + Environment.NewLine);
        }

        public static void WriteLine(Exception ex)
        {
            WriteLine(exceptionToString(ex));
        }

        public static void WriteLine(string format, params object[] args)
        {
            Write(string.Format(format, args) + Environment.NewLine);
        }


        public static void TraceEvent(string source, TraceEventType traceEventType, int id, string message)
        {
            TraceListener[] traceListeners;

            lock (_listeners)
            {
                traceListeners = _listeners.ToArray();
            }

            foreach (TraceListener t in traceListeners)
            {
                t.TraceEvent(new TraceEventCache(), source, traceEventType, id, message);
                if (_flushOnWrite)
                    t.Flush();
            }
        }

        static string exceptionToString(Exception ex)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine(ex.Message);
            sb.AppendLine(ex.StackTrace);
            if (ex.InnerException != null)
                sb.AppendLine(exceptionToString(ex.InnerException));

            return sb.ToString();
        }

        static void CreateDirectoryIfNotExists(string path)
        {
            if (string.IsNullOrEmpty(path))
                return;

            string directoryPath = Path.GetDirectoryName(path);
            if (!string.IsNullOrEmpty(directoryPath) && !Directory.Exists(directoryPath))
                Directory.CreateDirectory(directoryPath);
        }


    }
}
