using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace Utils
{
    public static class Functions
    {
        public static int CalculateHash(string mystring)
{
            MD5 md5Hasher = MD5.Create();
            var hashed = md5Hasher.ComputeHash(Encoding.UTF8.GetBytes(mystring));
            return BitConverter.ToInt32(hashed, 0);
        }

        public static List<string> SpiltIntoWords(string text)
        {
            var punctuation = text.Where(Char.IsPunctuation).Distinct().ToArray();
            var words = text.Split().Select(x => x.Trim(punctuation));
            return words.ToList();
        }

        public static void CheckNotNull(Object obj)
        {
            if (obj == null)
            {
                throw new NullReferenceException();
            }
        }
    }
}
