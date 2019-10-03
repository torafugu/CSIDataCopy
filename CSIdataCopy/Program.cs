using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;

namespace CSIdataCopy
{
    class Program
    {
        static void Main(string[] args)
        {
            int repeatNum = Convert.ToInt32(args[0]);

            // Create co_mst_new.csv
            string TABLE_NAME1 = "co_mst";
            string[] FIELD_ID1 = { "site_ref", "type", "co_num", "est_num", "cust_num", "cust_seq", "contact", "phone", "cust_po", "order_date", "taken_by", "terms_code", "ship_code", "price", "weight", "qty_packages", "freight", "misc_charges", "prepaid_amt", "sales_tax", "stat", "cost", "close_date", "freight_t", "m_charges_t", "prepaid_t", "sales_tax_t", "slsman", "eff_date", "exp_date", "whse", "sales_tax_2", "sales_tax_t2", "edi_order", "trans_nat", "process_ind", "delterm", "use_exch_rate", "tax_code1", "tax_code2", "frt_tax_code1", "frt_tax_code2", "msc_tax_code1", "msc_tax_code2", "discount_type", "disc_amount", "disc", "pricecode", "ship_partial", "ship_early", "matl_cost_t", "lbr_cost_t", "fovhd_cost_t", "vovhd_cost_t", "out_cost_t", "end_user_type", "exch_rate", "fixed_rate", "orig_site", "lcr_num", "edi_type", "invoiced", "credit_hold", "credit_hold_date", "credit_hold_reason", "credit_hold_user", "sync_reqd", "projected_date", "order_source", "convert_type", "aps_pull_up", "consolidate", "inv_freq", "summarize", "NoteExistsFlag", "RecordDate", "RowPointer", "einvoice", "charfld1", "charfld2", "charfld3", "datefld", "decifld1", "decifld2", "decifld3", "logifld", "ack_stat", "config_id", "CreatedBy", "UpdatedBy", "CreateDate", "InWorkflow", "include_tax_in_price", "trans_nat_2", "apply_to_inv_num", "export_type", "external_confirmation_ref", "is_external", "prospect_id", "opp_id", "lead_id", "days_shipped_before_due_date_tolerance", "days_shipped_after_due_date_tolerance", "shipped_over_ordered_qty_tolerance", "shipped_under_ordered_qty_tolerance", "consignment", "priority", "demanding_site", "demanding_site_po_num", "shipment_approval_required", "portal_order", "ship_hold", "payment_method", "ship_method", "surcharge", "surcharge_t", "config_doc_id", "curr_code", "external_whse_line_changed", "Uf_co_desc", "Uf_contact_type", "Uf_dept", "Uf_div_num", "Uf_product_code", "Uf_proj_type", "Uf_region" };
            string[] DATA_TYPE1 = { "nvarchar", "nchar", "nvarchar", "nvarchar", "nvarchar", "int", "nvarchar", "nvarchar", "nvarchar", "datetime", "nvarchar", "nvarchar", "nvarchar", "decimal", "decimal", "smallint", "decimal", "decimal", "decimal", "decimal", "nchar", "decimal", "datetime", "decimal", "decimal", "decimal", "decimal", "nvarchar", "datetime", "datetime", "nvarchar", "decimal", "decimal", "tinyint", "nvarchar", "nvarchar", "nvarchar", "tinyint", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "nchar", "decimal", "decimal", "nvarchar", "tinyint", "tinyint", "decimal", "decimal", "decimal", "decimal", "decimal", "nvarchar", "decimal", "tinyint", "nvarchar", "nvarchar", "nchar", "tinyint", "tinyint", "datetime", "nvarchar", "nvarchar", "tinyint", "datetime", "nvarchar", "nchar", "tinyint", "tinyint", "nchar", "tinyint", "tinyint", "datetime", "uniqueidentifier", "tinyint", "nvarchar", "nvarchar", "nvarchar", "datetime", "decimal", "decimal", "decimal", "tinyint", "nchar", "nvarchar", "nvarchar", "nvarchar", "datetime", "tinyint", "tinyint", "nvarchar", "nvarchar", "nchar", "nvarchar", "tinyint", "nvarchar", "nvarchar", "nvarchar", "smallint", "smallint", "decimal", "decimal", "tinyint", "smallint", "nvarchar", "nvarchar", "tinyint", "tinyint", "tinyint", "nvarchar", "nvarchar", "decimal", "decimal", "nvarchar", "nvarchar", "tinyint", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "nvarchar" };
            int[] TO_CHANGE_KEY_POS1 = { 2 };
            createNewFileWithSingleKey(TABLE_NAME1, FIELD_ID1, DATA_TYPE1, TO_CHANGE_KEY_POS1, repeatNum);

            // Create coitem_mst.csv
            string TABLE_NAME2 = "coitem_mst";
            string[] FIELD_ID2 = { "site_ref","co_num","co_line","co_release","item","qty_ordered","qty_ready","qty_shipped","qty_packed","disc","cost","price","ref_type","ref_num","ref_line_suf","ref_release","due_date","ship_date","reprice","cust_item","qty_invoiced","qty_returned","cgs_total","feat_str","stat","cust_num","cust_seq","prg_bill_tot","prg_bill_app","release_date","promise_date","whse","wks_basis","wks_value","comm_code","trans_nat","process_ind","delterm","unit_weight","origin","cons_num","tax_code1","tax_code2","export_value","ec_code","transport","pick_date","pricecode","u_m","qty_ordered_conv","price_conv","co_cust_num","packed","bol","qty_rsvd","matl_cost","lbr_cost","fovhd_cost","vovhd_cost","out_cost","cgs_total_matl","cgs_total_lbr","cgs_total_fovhd","cgs_total_vovhd","cgs_total_out","cost_conv","matl_cost_conv","lbr_cost_conv","fovhd_cost_conv","vovhd_cost_conv","out_cost_conv","ship_site","sync_reqd","co_orig_site","cust_po","rma_num","rma_line","projected_date","consolidate","inv_freq","summarize","NoteExistsFlag","RecordDate","RowPointer","description","config_id","CreatedBy","UpdatedBy","CreateDate","InWorkflow","rcpt_rqmt","trans_nat_2","suppl_qty_conv_factor","print_kit_components","due_date_day","external_reservation_ref","non_inv_acct","non_inv_acct_unit1","non_inv_acct_unit2","non_inv_acct_unit3","non_inv_acct_unit4","days_shipped_before_due_date_tolerance","days_shipped_after_due_date_tolerance","shipped_over_ordered_qty_tolerance","shipped_under_ordered_qty_tolerance","priority","invoice_hold","manufacturer_id","manufacturer_item","qty_picked","fs_inc_num","promotion_code","external_shipment_line_id","last_external_shipment_doc_id","last_process_shipment_doc_id","allow_on_pick_list","external_shipment_status","Uf_due_date_lt","Uf_proj_num","Uf_task_num","Uf_ObjectNoteToken" };
            string[] DATA_TYPE2 = { "nvarchar", "nvarchar", "smallint", "smallint", "nvarchar", "decimal", "decimal", "decimal", "decimal", "decimal", "decimal", "decimal", "nchar", "nvarchar", "smallint", "smallint", "datetime", "datetime", "tinyint", "nvarchar", "decimal", "decimal", "decimal", "nvarchar", "nchar", "nvarchar", "int", "decimal", "decimal", "datetime", "datetime", "nvarchar", "nchar", "decimal", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "decimal", "nvarchar", "int", "nvarchar", "nvarchar", "decimal", "nvarchar", "nvarchar", "datetime", "nvarchar", "nvarchar", "decimal", "decimal", "nvarchar", "tinyint", "tinyint", "decimal", "decimal", "decimal", "decimal", "decimal", "decimal", "decimal", "decimal", "decimal", "decimal", "decimal", "decimal", "decimal", "decimal", "decimal", "decimal", "decimal", "nvarchar", "tinyint", "nvarchar", "nvarchar", "nvarchar", "smallint", "datetime", "tinyint", "nchar", "tinyint", "tinyint", "datetime", "uniqueidentifier", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "datetime", "tinyint", "", "nvarchar", "float", "tinyint", "", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "smallint", "smallint", "decimal", "decimal", "smallint", "tinyint", "nvarchar", "nvarchar", "decimal", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "tinyint", "nchar", "datetime", "nvarchar", "int", "decimal" };
            int[] TO_CHANGE_KEY_POS2 = { 1 };
            createNewFileWithSingleKey(TABLE_NAME2, FIELD_ID2, DATA_TYPE2, TO_CHANGE_KEY_POS2, repeatNum);

            // Create po_mst.csv
            string TABLE_NAME3 = "po_mst";
            string[] FIELD_ID3 = { "site_ref", "po_num", "vend_num", "order_date", "po_cost", "ship_code", "terms_code", "fob", "print_price", "vend_order", "misc_charges", "sales_tax", "freight", "stat", "inv_date", "inv_num", "dist_date", "type", "drop_ship_no", "drop_seq", "eff_date", "exp_date", "ship_addr", "m_charges_t", "sales_tax_t", "freight_t", "whse", "sales_tax_2", "sales_tax_t2", "charfld1", "charfld2", "charfld3", "datefld", "decifld1", "decifld2", "decifld3", "logifld", "duty_amt_t", "duty_amt", "brokerage_amt", "brokerage_amt_t", "trans_nat", "process_ind", "delterm", "tax_code1", "tax_code2", "frt_tax_code1", "frt_tax_code2", "msc_tax_code1", "msc_tax_code2", "prepaid_amt", "prepaid_t", "exch_rate", "fixed_rate", "freight_vend_num", "duty_vend_num", "brokerage_vend_num", "frt_alloc_percent", "duty_alloc_percent", "brk_alloc_percent", "est_freight", "act_freight", "est_brokerage", "act_brokerage", "est_duty", "act_duty", "duty_alloc_meth", "frt_alloc_meth", "brk_alloc_meth", "duty_alloc_type", "frt_alloc_type", "brk_alloc_type", "vend_lcr_num", "received", "NoteExistsFlag", "RecordDate", "RowPointer", "buyer", "CreatedBy", "UpdatedBy", "CreateDate", "InWorkflow", "ins_vend_num", "ins_alloc_percent", "ins_alloc_type", "est_insurance", "ins_alloc_meth", "act_insurance", "insurance_amt", "insurance_amt_t", "loc_frt_vend_num", "loc_frt_alloc_percent", "loc_frt_alloc_type", "est_local_freight", "loc_frt_alloc_meth", "act_local_freight", "local_freight_amt", "local_freight_amt_t", "include_tax_in_cost", "trans_nat_2", "contains_only_tax_free_matls", "supply_web_po_stat", "builder_po_orig_site", "builder_po_num", "builder_po_printed", "synchronized_to_bus", "auto_voucher", "auto_receive_demanding_site_po", "auto_ship_demanding_site_co", "source_site_co_num", "curr_code" };
            string[] DATA_TYPE3 = { "nvarchar", "nvarchar", "nvarchar", "datetime", "decimal", "nvarchar", "nvarchar", "nvarchar", "tinyint", "nvarchar", "decimal", "decimal", "decimal", "nchar", "datetime", "nvarchar", "datetime", "nchar", "nvarchar", "int", "datetime", "datetime", "nchar", "decimal", "decimal", "decimal", "nvarchar", "decimal", "decimal", "nvarchar", "nvarchar", "nvarchar", "datetime", "decimal", "decimal", "decimal", "tinyint", "decimal", "decimal", "decimal", "decimal", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "decimal", "decimal", "decimal", "tinyint", "nvarchar", "nvarchar", "nvarchar", "decimal", "decimal", "decimal", "decimal", "decimal", "decimal", "decimal", "decimal", "decimal", "nchar", "nchar", "nchar", "nchar", "nchar", "nchar", "nvarchar", "tinyint", "tinyint", "datetime", "uniqueidentifier", "nvarchar", "nvarchar", "nvarchar", "datetime", "tinyint", "nvarchar", "decimal", "nchar", "decimal", "nchar", "decimal", "decimal", "decimal", "nvarchar", "decimal", "nchar", "decimal", "nchar", "decimal", "decimal", "decimal", "tinyint", "nvarchar", "tinyint", "nchar", "nvarchar", "nvarchar", "tinyint", "tinyint", "tinyint", "tinyint", "tinyint", "nvarchar", "nvarchar" };
            int[] TO_CHANGE_KEY_POS3 = { 1 };
            createNewFileWithSingleKey(TABLE_NAME3, FIELD_ID3, DATA_TYPE3, TO_CHANGE_KEY_POS3, repeatNum);

            // Create poitem_mst.csv
            string TABLE_NAME4 = "poitem_mst";
            string[] FIELD_ID4 = { "site_ref", "po_num", "po_line", "po_release", "rcvd_date", "date_seq", "qty_received", "qty_returned", "qty_vouchered", "item_cost", "unit_weight", "unit_mat_cost", "unit_duty_cost", "unit_freight_cost", "unit_brokerage_cost", "by_cons", "exch_rate", "pack_num", "grn_line", "grn_num", "vend_num", "NoteExistsFlag", "RecordDate", "RowPointer", "CreatedBy", "UpdatedBy", "CreateDate", "InWorkflow", "unit_insurance_cost", "unit_loc_frt_cost", "TH_vend_inv_num" };
            string[] DATA_TYPE4 = { "nvarchar", "nvarchar", "smallint", "smallint", "nvarchar", "nchar", "decimal", "decimal", "decimal", "decimal", "decimal", "decimal", "nchar", "nvarchar", "smallint", "smallint", "datetime", "datetime", "nvarchar", "nvarchar", "smallint", "decimal", "decimal", "nvarchar", "nvarchar", "int", "nchar", "datetime", "datetime", "nvarchar", "nchar", "nchar", "nvarchar", "nvarchar", "nvarchar", "decimal", "nvarchar", "nvarchar", "int", "decimal", "decimal", "decimal", "decimal", "nvarchar", "nvarchar", "decimal", "nvarchar", "nvarchar", "nvarchar", "smallint", "nvarchar", "decimal", "decimal", "decimal", "decimal", "decimal", "decimal", "decimal", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "tinyint", "nvarchar", "nvarchar", "nvarchar", "tinyint", "datetime", "uniqueidentifier", "nvarchar", "nvarchar", "nvarchar", "datetime", "tinyint", "tinyint", "", "decimal", "decimal", "decimal", "decimal", "nvarchar", "float", "tinyint", "", "tinyint", "tinyint", "nvarchar", "decimal", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "nvarchar" };
            int[] TO_CHANGE_KEY_POS4 = { 1 };
            createNewFileWithSingleKey(TABLE_NAME4, FIELD_ID4, DATA_TYPE4, TO_CHANGE_KEY_POS4, repeatNum);

            // Create po_rcpt_mst.csv
            string TABLE_NAME5 = "po_rcpt_mst";
            string[] FIELD_ID5 = { "site_ref", "po_num", "po_line", "po_release", "rcvd_date", "date_seq", "qty_received", "qty_returned", "qty_vouchered", "item_cost", "unit_weight", "unit_mat_cost", "unit_duty_cost", "unit_freight_cost", "unit_brokerage_cost", "by_cons", "exch_rate", "pack_num", "grn_line", "grn_num", "vend_num", "NoteExistsFlag", "RecordDate", "RowPointer", "CreatedBy", "UpdatedBy", "CreateDate", "InWorkflow", "unit_insurance_cost", "unit_loc_frt_cost", "TH_vend_inv_num" };
            string[] DATA_TYPE5 = { "nvarchar", "nvarchar", "smallint", "smallint", "datetime", "smallint", "decimal", "decimal", "decimal", "decimal", "decimal", "decimal", "decimal", "decimal", "decimal", "tinyint", "decimal", "nvarchar", "smallint", "nvarchar", "nvarchar", "tinyint", "datetime", "uniqueidentifier", "nvarchar", "nvarchar", "datetime", "tinyint", "decimal", "decimal", "nvarchar" };
            int[] TO_CHANGE_KEY_POS5 = { 1 };
            createNewFileWithSingleKey(TABLE_NAME5, FIELD_ID5, DATA_TYPE5, TO_CHANGE_KEY_POS5, repeatNum);

            // Create journal_mst.csv
            string TABLE_NAME6 = "journal_mst";
            string[] FIELD_ID6 = { "site_ref", "id", "seq", "trans_date", "acct", "dom_amount", "ref", "reverse", "vend_num", "inv_num", "voucher", "check_num", "check_date", "from_site", "vouch_seq", "ref_type", "matl_trans_num", "bank_code", "ref_num", "ref_line_suf", "ref_release", "acct_unit1", "acct_unit2", "acct_unit3", "acct_unit4", "curr_code", "exch_rate", "for_amount", "NoteExistsFlag", "RecordDate", "RowPointer", "CreatedBy", "UpdatedBy", "CreateDate", "InWorkflow", "control_prefix", "control_site", "control_year", "control_period", "control_number", "ref_control_prefix", "ref_control_site", "ref_control_year", "ref_control_period", "ref_control_number", "proj_trans_num", "cancellation" };
            string[] DATA_TYPE6 = { "nvarchar", "nvarchar", "int", "datetime", "nvarchar", "decimal", "nvarchar", "tinyint", "nvarchar", "nvarchar", "nvarchar", "int", "datetime", "nvarchar", "int", "nchar", "decimal", "nvarchar", "nvarchar", "smallint", "smallint", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "decimal", "decimal", "tinyint", "datetime", "uniqueidentifier", "nvarchar", "nvarchar", "datetime", "tinyint", "nvarchar", "nvarchar", "smallint", "tinyint", "decimal", "nvarchar", "nvarchar", "smallint", "tinyint", "decimal", "decimal", "tinyint" };
            int SEQ_POS6 = 2;
            int PO_POS6 = 18;
            createNewFileWithSeqPorder(TABLE_NAME6, TABLE_NAME3, TO_CHANGE_KEY_POS3[0], FIELD_ID6, DATA_TYPE6, SEQ_POS6, PO_POS6, repeatNum);

            // Create matltran_mst.csv
            string TABLE_NAME7 = "matltran_mst";
            string[] FIELD_ID7 = { "site_ref", "trans_num", "trans_type", "trans_date", "item", "qty", "whse", "loc", "ref_type", "ref_num", "ref_line_suf", "cost", "user_code", "lot", "ref_release", "reason_code", "backflush", "wc", "awaiting_eop", "matl_cost", "lbr_cost", "fovhd_cost", "vovhd_cost", "out_cost", "cost_code", "NoteExistsFlag", "RecordDate", "RowPointer", "CreatedBy", "UpdatedBy", "CreateDate", "InWorkflow", "document_num", "import_doc_id", "count_sequence", "emp_num", "job_lot", "job_ser_num", "TH_vend_inv_num", "date_seq" };
            string[] DATA_TYPE7 = { "nvarchar", "", "nchar", "datetime", "nvarchar", "decimal", "nvarchar", "nvarchar", "nchar", "nvarchar", "smallint", "decimal", "nvarchar", "nvarchar", "smallint", "nvarchar", "tinyint", "nvarchar", "tinyint", "decimal", "decimal", "decimal", "decimal", "decimal", "nvarchar", "tinyint", "datetime", "uniqueidentifier", "nvarchar", "nvarchar", "datetime", "tinyint", "nvarchar", "nvarchar", "decimal", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "smallint" };
            int SEQ_POS7 = 1;
            int PO_POS7 = 9;
            createNewFileWithSeqPorder(TABLE_NAME7, TABLE_NAME3, TO_CHANGE_KEY_POS3[0], FIELD_ID7, DATA_TYPE7, SEQ_POS7, PO_POS7, repeatNum);

            // Create matltran_amt_mst.csv
            string TABLE_NAME8 = "matltran_amt_mst";
            string[] FIELD_ID8 = { "site_ref", "trans_num", "trans_seq", "amt", "acct", "acct_unit1", "acct_unit2", "acct_unit3", "acct_unit4", "matl_amt", "matl_acct", "matl_acct_unit1", "matl_acct_unit2", "matl_acct_unit3", "matl_acct_unit4", "lbr_amt", "lbr_acct", "lbr_acct_unit1", "lbr_acct_unit2", "lbr_acct_unit3", "lbr_acct_unit4", "fovhd_amt", "fovhd_acct", "fovhd_acct_unit1", "fovhd_acct_unit2", "fovhd_acct_unit3", "fovhd_acct_unit4", "vovhd_amt", "vovhd_acct", "vovhd_acct_unit1", "vovhd_acct_unit2", "vovhd_acct_unit3", "vovhd_acct_unit4", "out_amt", "out_acct", "out_acct_unit1", "out_acct_unit2", "out_acct_unit3", "out_acct_unit4", "NoteExistsFlag", "RecordDate", "RowPointer", "CreatedBy", "UpdatedBy", "CreateDate", "InWorkflow", "include_in_inventory_bal_calc" };
            string[] DATA_TYPE8 = { "nvarchar", "decimal", "smallint", "decimal", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "decimal", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "decimal", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "decimal", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "decimal", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "decimal", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "nvarchar", "tinyint", "datetime", "uniqueidentifier", "nvarchar", "nvarchar", "datetime", "tinyint", "tinyint" };
            int SEQ_POS8 = 1;
            int PO_POS8 = 999;
            createNewFileWithSeqPorder(TABLE_NAME8, TABLE_NAME3, TO_CHANGE_KEY_POS3[0], FIELD_ID8, DATA_TYPE8, SEQ_POS8, PO_POS8, repeatNum);

        }

        private static void createNewFileWithSingleKey(string tableName, string[] fieldID, string[] dataType, int[] keyPos, int repeatNum)
        {
            String inputCSVfilename = tableName + ".csv";
            String outputCSVfilename = System.IO.Path.GetFileNameWithoutExtension(inputCSVfilename) + "_new.csv";
            Console.WriteLine("input CSV file is " + inputCSVfilename);
            Console.WriteLine("output CSV file is " + outputCSVfilename);

            using (StreamReader reader = new StreamReader(inputCSVfilename, Encoding.GetEncoding("shift_jis")))
            using (StreamWriter writer = new StreamWriter(outputCSVfilename, false, Encoding.GetEncoding("shift_jis")))
            {
                string originalLine = reader.ReadLine();

                for (int rowIndex = 0; rowIndex < repeatNum; rowIndex++)
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

                    string[] originalLineArr = originalLine.Split(',');

                    // create insert statement 1
                    for (int i2 = 0; i2 < originalLineArr.Length; i2++)
                    {
                        if (dataType[i2] == "") continue;
                        string value = "";
                        if (-1 < Array.IndexOf(keyPos, i2))
                        {
                            value = createNewKeyValue(originalLineArr[i2], rowIndex);
                        } else
                        {
                            value = originalLineArr[i2];
                        }

                        if (dataType[i2] == "nvarchar" || dataType[i2] == "nchar" || dataType[i2] == "datetime")
                        {
                            if (value != "NULL")
                            {
                                value = "'" + value + "'";
                            }
                        } else if (dataType[i2] == "uniqueidentifier") {
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
                }
                reader.Close();
            }
        }

        private static void createNewFileWithSeqPorder(string tableName, string poTableName, int poKeyPos, string[] fieldID, string[] dataType, int seqPos, int poNumPos, int repeatNum)
        {
            String inputPOCSVfilename = poTableName + ".csv";
            String poKey = "";
            using (StreamReader reader = new StreamReader(inputPOCSVfilename, Encoding.GetEncoding("shift_jis")))
            {
                string firstlLine = reader.ReadLine();
                poKey = firstlLine.Split(',')[poKeyPos];
                reader.Close();
            }

            String inputCSVfilename = tableName + ".csv";
            String outputCSVfilename = System.IO.Path.GetFileNameWithoutExtension(inputCSVfilename) + "_new.csv";
            Console.WriteLine("input CSV file is " + inputCSVfilename);
            Console.WriteLine("output CSV file is " + outputCSVfilename);

            using (StreamReader reader = new StreamReader(inputCSVfilename, Encoding.GetEncoding("shift_jis")))
            using (StreamWriter writer = new StreamWriter(outputCSVfilename, false, Encoding.GetEncoding("shift_jis")))
            {
                string originalLine = reader.ReadLine();

                for (int rowIndex = 0; rowIndex < repeatNum; rowIndex++)
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

                    string[] originalLineArr = originalLine.Split(',');

                    // create insert statement 1
                    for (int i2 = 0; i2 < originalLineArr.Length; i2++)
                    {
                        if (dataType[i2] == "") continue;
                        string value = "";
                        if (seqPos == i2)
                        {
                            value = (Convert.ToInt32(originalLineArr[i2].ToString()) + rowIndex + 1).ToString();
                        }
                        else if (poNumPos == i2)
                        {
                            value = poKey;
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
                }
                reader.Close();
            }
        }

        private static string createNewKeyValue(string originalKey, int rowIndex)
        {
            Console.WriteLine("Original key is " + originalKey);
            Match originalKeyHalf1 = Regex.Match(originalKey, "^[A-Z]*");
            Match originalKeyHalf2 = Regex.Match(originalKey, "[0-9]*$");
            string newKey = originalKeyHalf1.Value + (Convert.ToInt32(originalKeyHalf2.Value) + rowIndex + 1).ToString();
            Console.WriteLine("New key is " + newKey);

            return newKey;
        }
    }
}
