using System;
using System.Text;

namespace SearchThroughput
{
    public class WordGenerator
    {
        Random rng = new Random(15);

        string alphabet = "abcdefghijklmnopqrstuvwxyz";

        public WordGenerator()
        {
        }

        public WordGenerator(string alphabet)
        {
            this.alphabet = alphabet;
        }

        public string NextWord(int length)
        {
            char[] chars = new char[length];

            for (int i = 0; i < chars.Length; i++)
            {
                chars[i] = this.alphabet[this.rng.Next(this.alphabet.Length)];
            }

            return new string(chars);
        }

        public string BuildText(int length, int wordLength, int variation)
        {
            StringBuilder builder = new StringBuilder(length + wordLength);
            while (builder.Length < length)
            {
                if (builder.Length > 0)
                {
                    builder.Append(" ");
                }
                builder.Append(this.NextWord(this.rng.Next(wordLength - variation, wordLength + variation)));
            }

            return builder.ToString();
        }
    }
}