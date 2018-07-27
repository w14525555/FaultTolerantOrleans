using Orleans;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using SystemInterfaces;
using SystemInterfaces.Model;

namespace SystemImplementation
{
    //To generate random sentences
    public class SentenceGenerator: Grain, ISentenceGenerator
    {
        private List<string> words = new List<string>(new string[] { "an", "automobile", "or", "motor", "car", "is", "a", "wheeled", "motor", "vehicle", "used", "for", "transporting", "passengers", "which", "also", "carries", "its", "own", "engine", "or" });
        private List<IStreamSource> sources = new List<IStreamSource>();
        private static Random random = new Random();
        private TimeSpan sentenceInterval = new TimeSpan(1250);

        private IDisposable disposable;

        public Task RegisterTimerAndSetSources(List<IStreamSource> sources)
        {
            this.sources = sources; 
            disposable = RegisterTimer(GenerateAndSendSentences, null, sentenceInterval, sentenceInterval);
            return Task.CompletedTask;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Await.Warning", "CS4014:Await.Warning")]
        private async Task<Task> GenerateAndSendSentences(object org)
        {
            string sentence = GetRandomSentence();
            var message = new StreamMessage("message", sentence);
            foreach (var source in sources)
            {
                source.ProduceMessageAsync(message);
            }

            return Task.CompletedTask;
        }

        private string GetRandomSentence()
        {
            //At first get random length
            int length = random.Next(1, 10);
            string sentence = "";
            for (int i = 0; i < length; i++)
            {
                sentence = sentence + GetRandomWord();
            }
            return sentence;
        }

        private string GetRandomWord()
        {
            int index = random.Next(words.Count-1);
            return words[index] + " ";
        }
        
    }
}
