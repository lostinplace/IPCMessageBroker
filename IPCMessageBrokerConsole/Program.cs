using IPCMessageBroker.ConsumerRecordTracking;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace IPCMessageBrokerConsole
{
  class Command
  {
    public Regex pattern;
    public Func<Match, Task<string>> function;
    
  }

  public static class Extensions
  {
    public static Func<Match, Task<string>> ToAsyncFunction(this Func<Match, string> func)
    {
      return async (_) =>
      {
        return await Task.FromResult(func(_));
      };
    }
  }


  class Program
  {
    

    private static string ApplicationName { get; set; } = "";



    private static List<Command> commands = new List<Command>()
    {
      new Command()
      {
        pattern = new Regex("get consumer_record_size"),
        function = Extensions.ToAsyncFunction((_)=>{

          if(ApplicationName == "")
          {
            return "must specify application_name";
          }
          return IPCMessageBroker.ConsumerRecordTracking.ConsumerRecordTracking.GetConsumerRecordSize(ApplicationName).ToString();
        })
      },
      new Command {
        pattern = new Regex(@"initialize consumer_header"),
        function = Extensions.ToAsyncFunction(
        (match)=>{
          if(ApplicationName == "")
          {
            return "must specify application_name";
          }
          IPCMessageBroker.ConsumerRecordTracking.ConsumerRecordTracking.InitializeConsumerRecordHeaderCache(ApplicationName);
          return $"consumer_records for '{ApplicationName}' have been initialized";
        })
      },
      new Command {
        pattern = new Regex(@"get last_written_index"),
        function = 
        async (match)=>{
          if(ApplicationName == "")
          {
            return "must specify application_name";
          }
          var result = IPCMessageBroker.ConsumerRecordTracking.ConsumerRecordTracking.GetLastWrittenIndex(ApplicationName);
          return $"last written index for '{ApplicationName}' was {await result}";
        }
      },
      new Command {
        pattern = new Regex(@"increment last_written_index"),
        function =
        async (match)=>{
          if(ApplicationName == "")
          {
            return "must specify application_name";
          }
          var result = IPCMessageBroker.ConsumerRecordTracking.ConsumerRecordTracking.IncrementLastWrittenIndex(ApplicationName);
          return $"last written index for '{ApplicationName}' was {await result}";
        }
      },
      new Command {
        pattern = new Regex(@"set application_name (\S+)"),
        function =  Extensions.ToAsyncFunction(
        (match)=>{

          ApplicationName = match.Groups[1].Value;
          return $"set application name to '{ApplicationName}'";
        })
      },
      new Command {
        pattern = new Regex(@"get application_name"),
        function =  Extensions.ToAsyncFunction(
        (match)=>{
          return $"application_name is '{ApplicationName}'";
        })
      },
      
      new Command {
        pattern = new Regex(@".*"),
        function =  Extensions.ToAsyncFunction(
        (match)=>{

          var commandStrings = commands.Select(x => x.pattern.ToString()).ToList();
          var commandOutput = string.Join('\n', commandStrings );
          return $"command, '{match.Value}' not recognized\nAvailable Commands:\n{commandOutput}";
        })
      }

    };


    public async static Task<string> interpretInput(string input)
    {
      var command = commands.First(x => x.pattern.IsMatch(input));
      var match = command.pattern.Match(input);
      string result = await command.function(match);
      return $"\n----\n{result}\n-----\n";

    }

    static void Main(string[] args)
    {
      Console.WriteLine("IPC Message Broker Console");
      string input;
      do
      {
        input = Console.ReadLine();
        var thisTask = interpretInput(input);
        thisTask.Wait();
        Console.WriteLine(thisTask.Result);
      } while (input != "exit");


    }
  }
}
