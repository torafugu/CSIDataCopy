using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace CSIdataCopy
{
    class Program
    {
        const string FIELD_SUFFIX = "_field.csv";
        const string TYPE_SUFFIX = "_type.csv";
        static string[] items = loadMaster("item", 1);
        static string[] customers = loadMaster("customer", 1);
        static string[] vendors = loadMaster("vendor", 1);
        static System.Random r = new System.Random();

        static void Main(string[] args)
        {
            int repeatNum = Convert.ToInt32(args[0]);

            DateTime startDate = new DateTime(int.Parse(args[1].Substring(0, 4)), int.Parse(args[1].Substring(4, 2)), int.Parse(args[1].Substring(6, 2)));
            int dateRange = int.Parse(args[2]);
            Console.WriteLine(startDate.AddDays(r.Next(dateRange)).ToString());

            // Create co_mst_new.csv
            string tableName = "co_mst";
            int[] keyPos = { 2 };
            string[] keyTypes = { "c" };
            int datePos = 9;
            int[] repeatNums = { repeatNum, 1};
            Console.WriteLine(tableName + " started.");
            createInsertStatement(tableName, keyPos, keyTypes, datePos, startDate, dateRange, repeatNums);

            // create coitem_mst.csv
            tableName = "coitem_mst";
            keyPos = new int[] { 1, 2 };
            keyTypes = new string[] { "c", "n" };
            datePos = 16;
            repeatNums = new int[] { repeatNum, 10};
            Console.WriteLine(tableName + " started.");
            createInsertStatement(tableName, keyPos, keyTypes, datePos, startDate, dateRange, repeatNums);

            // Create po_mst.csv
            tableName = "po_mst";
            keyPos = new int[] { 1 };
            keyTypes = new string[] { "c" };
            datePos = 3;
            repeatNums = new int[] { repeatNum, 1};
            Console.WriteLine(tableName + " started.");
            createInsertStatement(tableName, keyPos, keyTypes, datePos, startDate, dateRange, repeatNums);

            // Create poitem_mst.csv
            tableName = "poitem_mst";
            keyPos = new int[] { 1, 2, 3 };
            keyTypes = new string[] { "c", "n", "n" };
            datePos = 16;
            repeatNums = new int[] { repeatNum, 10};
            Console.WriteLine(tableName + " started.");
            List<List<string>> generatedKeys = createInsertStatement(tableName, keyPos, keyTypes, datePos, startDate, dateRange, repeatNums);

            // Create po_rcpt_mst.csv
            tableName = "po_rcpt_mst";
            int[] destKeyPos = { 1, 2, 3 };
            datePos = 4;
            int newKeyPos = 5;
            string newKeyType = "p";
            Console.WriteLine(tableName + " started.");
            generatedKeys = createInsertStatement(generatedKeys, tableName, destKeyPos, newKeyPos, newKeyType, datePos, startDate, dateRange, 3, true);

            //Console.WriteLine("Printing generatedKeys");
            //for (int i = 0; i < generatedKeys.Count; i++)
            //{
            //    for (int j = 0; j < generatedKeys[i].Count; j++)
            //    {
            //        //Console.WriteLine(i + "," +j);
            //        Console.WriteLine(generatedKeys[i][j]);
            //    }
            //}

            // Create journal_mst.csv
            tableName = "journal_mst";
            destKeyPos = new int[] { 18, 19, 20 };
            datePos = 3;
            newKeyPos = 2;
            newKeyType = "a";
            Console.WriteLine(tableName + " started.");
            createInsertStatement(generatedKeys, tableName, destKeyPos, newKeyPos, newKeyType, datePos, startDate, dateRange, 2, false);

            //// Create matltran_mst.csv
            tableName = "matltran_mst";
            destKeyPos = new int[] { 9, 10, 11 };
            datePos = 3;
            newKeyPos = -1;
            newKeyType = "a";
            Console.WriteLine(tableName + " started.");
            createInsertStatement(generatedKeys, tableName, destKeyPos, newKeyPos, newKeyType, datePos, startDate, dateRange, 1, false);

            //// Create matltran_amt_mst.csv
            tableName = "matltran_amt_mst";
            keyPos = new int[] { 1, 2 };
            keyTypes = new string[] { "c", "n" };
            datePos = -1;
            repeatNums = new int[] { generatedKeys.Count, 1 };
            Console.WriteLine(tableName + " started.");
            createInsertStatement(tableName, keyPos, keyTypes, datePos, startDate, dateRange, repeatNums);
        }

        private static string[] loadMaster(string masterName, int columnNum)
        {
            String inputTSVfilename = masterName + "_mst";
            var valueList = new ArrayList();
            

            using (StreamReader reader = new StreamReader(inputTSVfilename, Encoding.GetEncoding("shift_jis")))
            {
                // Skip first row because of the header
                reader.ReadLine();

                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    if (line.Split('\t').Length - 1 < columnNum)
                    {
                        continue;
                    }
                    valueList.Add(line.Split('\t')[columnNum]);
                }
                reader.Close();
            }
            return (string[])valueList.ToArray(typeof(string));
        }

        private static List<List<string>> createInsertStatement(string tableName, int[] keyPos, string[] keyTypes, int datePos, DateTime startDate, int dateRange, int[] repeatNums)
        {
            List<List<string>> generatedKeys = new List<List<string>>();
            String inputCSVfilename = tableName + ".csv";
            String outputCSVfilename = System.IO.Path.GetFileNameWithoutExtension(inputCSVfilename) + "_new.csv";

            string[] fieldID = getStringArrayFromCSV(tableName + FIELD_SUFFIX);
            string[] dataType = getStringArrayFromCSV(tableName + TYPE_SUFFIX);

            using (StreamReader reader = new StreamReader(inputCSVfilename, Encoding.GetEncoding("shift_jis")))
            using (StreamWriter writer = new StreamWriter(outputCSVfilename, false, Encoding.GetEncoding("shift_jis")))
            {
                string originalLine = reader.ReadLine();

                for (int rowIndex1 = 0; rowIndex1 < repeatNums[0]; rowIndex1++)
                {
                    Console.WriteLine("RowIndex1 is (" + rowIndex1 + ")");
                    
                    int repeatNum2 = GetNormRandom(repeatNums[1] / 2, repeatNums[1] / 2);
                    if (repeatNum2 < 1) repeatNum2 = 1;

                    for (int rowIndex2 = 0; rowIndex2 < repeatNum2; rowIndex2++)
                    {
                        Console.WriteLine("RowIndex2 is (" + rowIndex2 + ")");
                        List<string> generatedKey = new List<string>();

                        // create insert statement 1 
                        StringBuilder insert_sb = new StringBuilder();
                        insert_sb.Append("INSERT INTO ");
                        insert_sb.Append(tableName);
                        insert_sb.Append(" (");

                        for (int i1 = 0; i1 < fieldID.Length; i1++)
                        {
                            if (dataType[i1] == "") continue;

                            insert_sb.Append(fieldID[i1]);
                            if (i1 < fieldID.Length - 1)
                            {
                                insert_sb.Append(",");
                            }
                        }

                        insert_sb.Append(") VALUES (");

                        string[] originalLineArr = originalLine.Split(',');

                        // create insert statement 1
                        for (int i2 = 0; i2 < originalLineArr.Length; i2++)
                        {
                            if (dataType[i2] == "") continue;

                            string value = "";
                            int keyIndex = Array.IndexOf(keyPos, i2);
                            if (-1 < keyIndex)
                            {
                                int rowIndex = rowIndex1;
                                if (keyIndex == 1)
                                {
                                    rowIndex = rowIndex2;
                                }

                                if (keyTypes[keyIndex] == "c")
                                {
                                    value = createNewKeyValue(originalLineArr[i2], rowIndex);
                                    generatedKey.Add(value);
                                }
                                else
                                {
                                    if (keyIndex == 1)
                                    {
                                        value = (Convert.ToInt32(originalLineArr[i2].ToString()) + rowIndex).ToString();
                                    } else
                                    {
                                        value = "0";
                                    }
                                    generatedKey.Add(value);
                                }
                            }
                            else if (i2 == datePos)
                            {
                                value = startDate.AddDays(r.Next(dateRange)).ToString();
                            }
                            else if (fieldID[i2] == "item")
                            {
                                value = items[r.Next(items.Length)];
                            }
                            else if (fieldID[i2] == "cust_num")
                            {
                                value = customers[r.Next(customers.Length)];
                            }
                            else if (fieldID[i2] == "vend_num")
                            {
                                value = vendors[r.Next(vendors.Length)];
                            }
                            else
                            {
                                value = originalLineArr[i2];
                            }

                            if (dataType[i2] == "nvarchar" || dataType[i2] == "nchar" || dataType[i2] == "datetime")
                            {
                                if (value != "NULL")
                                {
                                    value = "'" + value + "'";
                                }
                            }
                            else if (dataType[i2] == "uniqueidentifier")
                            {
                                value = "newid()";
                            }

                            insert_sb.Append(value);


                            if (i2 < originalLineArr.Length - 1)
                            {
                                insert_sb.Append(",");
                            }
                        }
                        insert_sb.Append(");");
                        writer.WriteLine(insert_sb.ToString());
                        generatedKeys.Add(generatedKey);
                    }
                }
                reader.Close();
                writer.Close();
            }
            return generatedKeys;
        }

        private static List<List<string>> createInsertStatement(List<List<string>> givenKeys, string tableName, int[] destKeyPos, int newKeyPos, string newKeyType, int datePos, DateTime startDate, int dateRange, int repeatNum, Boolean repeatRandomFlg)
        {
            List<List<string>> generatedKeys = new List<List<string>>();
            String srcCSVfilename = tableName + ".csv";          
            String outputCSVfilename = System.IO.Path.GetFileNameWithoutExtension(srcCSVfilename) + "_new.csv";

            string[] fieldID = getStringArrayFromCSV(tableName + FIELD_SUFFIX);
            string[] dataType = getStringArrayFromCSV(tableName + TYPE_SUFFIX);

            using (StreamReader reader = new StreamReader(srcCSVfilename, Encoding.GetEncoding("shift_jis")))
            using (StreamWriter writer = new StreamWriter(outputCSVfilename, false, Encoding.GetEncoding("shift_jis")))
            {
                string srcLine = reader.ReadLine();
                string[] srcLineArr = srcLine.Split(',');
                int allIndex = 1;

                for (int srcRowIndex = 0; srcRowIndex < givenKeys.Count; srcRowIndex++)
                {
                    Console.WriteLine("RowIndex is (" + srcRowIndex + ")");
                    List<string> generatedKey = new List<string>();

                    int randomRepeatNum = repeatNum;
                    if (repeatRandomFlg) randomRepeatNum = r.Next(repeatNum) + 1;

                    for (int rowIndex = 0; rowIndex < randomRepeatNum; rowIndex++)
                    {
                        // create insert statement 1 
                        StringBuilder insert_sb = new StringBuilder();
                        insert_sb.Append("INSERT INTO ");
                        insert_sb.Append(tableName);
                        insert_sb.Append(" (");

                        for (int i1 = 0; i1 < fieldID.Length; i1++)
                        {
                            if (dataType[i1] == "") continue;

                            insert_sb.Append(fieldID[i1]);
                            if (i1 < fieldID.Length - 1)
                            {
                                insert_sb.Append(",");
                            }
                        }

                        insert_sb.Append(") VALUES (");

                        // create insert statement 1
                        for (int i2 = 0; i2 < srcLineArr.Length; i2++)
                        {
                            if (dataType[i2] == "") continue;

                            string value = "";
                            int keyIndex = Array.IndexOf(destKeyPos, i2);
                            if (-1 < keyIndex)
                            {
                                value = givenKeys[srcRowIndex][keyIndex];
                                generatedKey.Add(value);
                            }
                            else if (i2 == newKeyPos)
                            {
                                if (newKeyType == "p" )
                                {
                                    value = (Convert.ToInt32(srcLineArr[i2].ToString()) + rowIndex).ToString();
                                } else
                                {
                                    value = (Convert.ToInt32(srcLineArr[i2].ToString()) + allIndex).ToString();
                                }
                                
                                generatedKey.Add(value);
                            }
                            else if (i2 == datePos)
                            {
                                value = startDate.AddDays(r.Next(dateRange)).ToString();
                            }
                            else if (fieldID[i2] == "item")
                            {
                                value = items[r.Next(items.Length)];
                            }
                            else if (fieldID[i2] == "cust_num")
                            {
                                value = customers[r.Next(customers.Length)];
                            }
                            else if (fieldID[i2] == "vend_num")
                            {
                                value = vendors[r.Next(vendors.Length)];
                            }
                            else
                            {
                                value = srcLineArr[i2];
                            }

                            if (dataType[i2] == "nvarchar" || dataType[i2] == "nchar" || dataType[i2] == "datetime")
                            {
                                if (value != "NULL")
                                {
                                    value = "'" + value + "'";
                                }
                            }
                            else if (dataType[i2] == "uniqueidentifier")
                            {
                                value = "newid()";
                            }

                            insert_sb.Append(value);


                            if (i2 < srcLineArr.Length - 1)
                            {
                                insert_sb.Append(",");
                            }
                        }

                        insert_sb.Append(");");
                        writer.WriteLine(insert_sb.ToString());
                        generatedKeys.Add(generatedKey);
                        allIndex++;
                    }
                }
                reader.Close();
                writer.Close();
            }
            return generatedKeys;
        }

        private static string[] getStringArrayFromCSV(string csvFileName)
        {
            string firstlLine = "";
            using (StreamReader reader = new StreamReader(csvFileName, Encoding.GetEncoding("shift_jis")))
            {
                firstlLine = reader.ReadLine().Replace("\"", "");
                reader.Close();
            }

            return firstlLine.Split(',');
        }

        private static string createNewKeyValue(string originalKey, int rowIndex)
        {
            //Console.WriteLine("Original key is " + originalKey);
            Match originalKeyHalf1 = Regex.Match(originalKey, "^[A-Z]*");
            Match originalKeyHalf2 = Regex.Match(originalKey, "[0-9]*$");
            string newKey = originalKeyHalf1.Value + (Convert.ToInt32(originalKeyHalf2.Value) + rowIndex + 1).ToString();
            //Console.WriteLine("New key is " + newKey);

            return newKey;
        }

        public static double GetRandom()
        {
            RNGCryptoServiceProvider rng = new RNGCryptoServiceProvider();
            byte[] bs = new byte[sizeof(Int32)];
            rng.GetBytes(bs);
            int iR = System.BitConverter.ToInt32(bs, 0);
            return ((double)iR / Int32.MaxValue);
        }

        public static double GetNormRandom()
        {
            double dR1 = Math.Abs(GetRandom());
            double dR2 = Math.Abs(GetRandom());
            return (Math.Sqrt(-2 * Math.Log(dR1, Math.E)) * Math.Cos(2 * Math.PI * dR2));
        }

        public static int GetNormRandom(int m, int s)
        {
            return (int)(m + s * GetNormRandom());
        }
    }
}
