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

            DateTime start = new DateTime(int.Parse(args[1].Substring(0, 4)), int.Parse(args[1].Substring(4, 2)), int.Parse(args[1].Substring(6, 2)));
            Console.WriteLine(start.AddDays(r.Next(int.Parse(args[2]))));

            // Create co_mst_new.csv
            string tableName = "co_mst";
            int[] keyPos = { 2 };
            string[] keyTypes = { "c" };
            int[] repeatNums = { repeatNum, 1};
            Console.WriteLine(tableName + " started." + " Repeat num is (" + repeatNums[0] + "," + repeatNums[1] + ")");
            createInsertStatement(tableName, keyPos, keyTypes, repeatNums);

            // create coitem_mst.csv
            tableName = "coitem_mst";
            keyPos = new int[] { 1, 2 };
            keyTypes = new string[] { "c", "n" };
            repeatNums = new int[] { repeatNum, 10};
            Console.WriteLine(tableName + " started." + " Repeat num is (" + repeatNums[0] + "," + repeatNums[1] + ")");
            createInsertStatement(tableName, keyPos, keyTypes, repeatNums);

            // Create po_mst.csv
            tableName = "po_mst";
            keyPos = new int[] { 1 };
            keyTypes = new string[] { "c" };
            repeatNums = new int[] { repeatNum, 1};
            Console.WriteLine(tableName + " started." + " Repeat num is (" + repeatNums[0] + "," + repeatNums[1] + ")");
            createInsertStatement(tableName, keyPos, keyTypes, repeatNums);

            // Create poitem_mst.csv
            tableName = "poitem_mst";
            keyPos = new int[] { 1, 2 };
            keyTypes = new string[] { "c", "n" };
            repeatNums = new int[] { repeatNum, 10};
            Console.WriteLine(tableName + " started." + " Repeat num is (" + repeatNums[0] + "," + repeatNums[1] + ")");
            List<List<string>> generatedKeys = createInsertStatement(tableName, keyPos, keyTypes, repeatNums);

            //Console.WriteLine("Printing generatedKeys");

            //for (int i = 0; i < generatedKeys.Count; i++)
            //{
            //    for (int j = 0; j < generatedKeys[i].Count; j++)
            //    {
            //        //Console.WriteLine(i + "," +j);
            //        Console.WriteLine(generatedKeys[i][j]);
            //    }
            //}

            // Create po_rcpt_mst.csv
            tableName = "po_rcpt_mst";
            int[] destKeyPos = { 1, 2 };
            int newKeyPos = 5;
            Console.WriteLine(tableName + " started." + " Repeat num is (" + destKeyPos[0] + "," + destKeyPos[1] + "," + repeatNum + ")");
            createInsertStatement(generatedKeys, tableName, destKeyPos, newKeyPos, 3);

            //// Create journal_mst.csv
            //string TABLE_NAME6 = "journal_mst";
            //int SEQ_POS6 = 2;
            //int PO_POS6 = 18;
            //createNewFileWithSeqPorder(TABLE_NAME6, TABLE_NAME3, TO_CHANGE_KEY_POS3[0], getStringArrayFromCSV(TABLE_NAME6 + FIELD_SUFFIX), getStringArrayFromCSV(TABLE_NAME6 + TYPE_SUFFIX), SEQ_POS6, PO_POS6, repeatNum);

            //// Create matltran_mst.csv
            //string TABLE_NAME7 = "matltran_mst";
            //int SEQ_POS7 = 1;
            //int PO_POS7 = 9;
            //createNewFileWithSeqPorder(TABLE_NAME7, TABLE_NAME3, TO_CHANGE_KEY_POS3[0], getStringArrayFromCSV(TABLE_NAME7 + FIELD_SUFFIX), getStringArrayFromCSV(TABLE_NAME7 + TYPE_SUFFIX), SEQ_POS7, PO_POS7, repeatNum);

            //// Create matltran_amt_mst.csv
            //string TABLE_NAME8 = "matltran_amt_mst";
            //int SEQ_POS8 = 1;
            //int PO_POS8 = 999;
            //createNewFileWithSeqPorder(TABLE_NAME8, TABLE_NAME3, TO_CHANGE_KEY_POS3[0], getStringArrayFromCSV(TABLE_NAME8 + FIELD_SUFFIX), getStringArrayFromCSV(TABLE_NAME8 + TYPE_SUFFIX), SEQ_POS8, PO_POS8, repeatNum);
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

        private static List<List<string>> createInsertStatement(string tableName, int[] keyPos, string[] keyTypes, int[] repeatNums)
        {
            List<List<string>> generatedKeys = new List<List<string>>();
            //System.Random r = new System.Random();

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
                                    value = (Convert.ToInt32(originalLineArr[i2].ToString()) + rowIndex).ToString();
                                    generatedKey.Add(value);
                                }
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

        private static void createInsertStatement(List<List<string>> generatedKeys, string tableName, int[] destKeyPos, int newKeyPos, int repeatNum)
        {
            //System.Random r = new System.Random();

            String srcCSVfilename = tableName + ".csv";          
            String outputCSVfilename = System.IO.Path.GetFileNameWithoutExtension(srcCSVfilename) + "_new.csv";

            string[] fieldID = getStringArrayFromCSV(tableName + FIELD_SUFFIX);
            string[] dataType = getStringArrayFromCSV(tableName + TYPE_SUFFIX);

            using (StreamReader reader = new StreamReader(srcCSVfilename, Encoding.GetEncoding("shift_jis")))
            using (StreamWriter writer = new StreamWriter(outputCSVfilename, false, Encoding.GetEncoding("shift_jis")))
            {
                string srcLine = reader.ReadLine();
                string[] srcLineArr = srcLine.Split(',');

                for (int srcRowIndex = 0; srcRowIndex < generatedKeys.Count; srcRowIndex++)
                {
                    Console.WriteLine("RowIndex is (" + srcRowIndex + ")");

                    for (int rowIndex = 0; rowIndex < r.Next(repeatNum); rowIndex++)
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
                                value = generatedKeys[srcRowIndex][keyIndex];
                            }
                            else if (i2 == newKeyPos)
                            {
                                value = (Convert.ToInt32(srcLineArr[i2].ToString()) + rowIndex).ToString();
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
                    }
                }
                reader.Close();
                writer.Close();
            }
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
