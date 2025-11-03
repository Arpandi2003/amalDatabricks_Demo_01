# Databricks notebook source
# MAGIC %md
# MAGIC **Notebook Name:** NB_Pace_Data <br>
# MAGIC **Created By:** Naveena <br>
# MAGIC **Created Date:** 12/08/2025<br>
# MAGIC **Modified By:** Naveena<br>
# MAGIC **Modified Date** 12/08/2025<br>
# MAGIC **Modification** :

# COMMAND ----------

# MAGIC %run "../General/NB_Configuration"

# COMMAND ----------

spark.sql(f"use catalog {catalog}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW finance.pace_balances AS
# MAGIC
# MAGIC WITH gldaily_import AS (
# MAGIC /* In future years, create new CTEs for new GLDaily tables and union them*/
# MAGIC SELECT *
# MAGIC FROM bronze.axiom_gldaily2025  
# MAGIC   ),
# MAGIC
# MAGIC acct_lookup_table AS (
# MAGIC   SELECT *
# MAGIC   FROM bronze.axiom_acct),
# MAGIC
# MAGIC /* Date solution pulls numbers (as a string) after D in Day column and adds to 2024-12-31. This isn't ultimately sustainable when bringing in multiple years
# MAGIC as Day column resets each year*/
# MAGIC pivoted AS (
# MAGIC SELECT CASE 
# MAGIC     WHEN a.ABReport_Det LIKE '%AFS%' THEN 'AFS'
# MAGIC     WHEN a.ABReport_Det LIKE '%HTM%' THEN 'HTM'
# MAGIC     ELSE NULL
# MAGIC   END AS Maturity_Type,
# MAGIC   CASE 
# MAGIC     WHEN a.ABReport_Det LIKE '%RPACE%' THEN 'RPACE'
# MAGIC     WHEN a.ABReport_Det LIKE '%CPACE%' THEN 'CPACE'
# MAGIC     ELSE NULL
# MAGIC   END AS Pace_Type,
# MAGIC    a.ABReport_Det AS ABReport_Det,
# MAGIC    a.Description as Description,
# MAGIC    a.ACCT AS ACCT_Number,
# MAGIC    gl.dept AS DEPT,
# MAGIC    DATE_ADD(DATE('2024-12-31'), CAST(SUBSTRING(Day, 2) AS INT)) AS Date, /* This is one line enclosed by DATE_ADD - base date, then # of days to add taken from later pivot*/
# MAGIC    DATE_TRUNC('month', Date) AS EntryMonth,
# MAGIC    Value
# MAGIC FROM gldaily_import gl
# MAGIC UNPIVOT (
# MAGIC   Value FOR Day IN (D1, D2, D3, D4, D5, D6, D7, D8, D9, D10,
# MAGIC D11, D12, D13, D14, D15, D16, D17, D18, D19, D20,
# MAGIC D21, D22, D23, D24, D25, D26, D27, D28, D29, D30,
# MAGIC D31, D32, D33, D34, D35, D36, D37, D38, D39, D40,
# MAGIC D41, D42, D43, D44, D45, D46, D47, D48, D49, D50,
# MAGIC D51, D52, D53, D54, D55, D56, D57, D58, D59, D60,
# MAGIC D61, D62, D63, D64, D65, D66, D67, D68, D69, D70,
# MAGIC D71, D72, D73, D74, D75, D76, D77, D78, D79, D80,
# MAGIC D81, D82, D83, D84, D85, D86, D87, D88, D89, D90,
# MAGIC D91, D92, D93, D94, D95, D96, D97, D98, D99, D100,
# MAGIC D101, D102, D103, D104, D105, D106, D107, D108, D109, D110,
# MAGIC D111, D112, D113, D114, D115, D116, D117, D118, D119, D120,
# MAGIC D121, D122, D123, D124, D125, D126, D127, D128, D129, D130,
# MAGIC D131, D132, D133, D134, D135, D136, D137, D138, D139, D140,
# MAGIC D141, D142, D143, D144, D145, D146, D147, D148, D149, D150,
# MAGIC D151, D152, D153, D154, D155, D156, D157, D158, D159, D160,
# MAGIC D161, D162, D163, D164, D165, D166, D167, D168, D169, D170,
# MAGIC D171, D172, D173, D174, D175, D176, D177, D178, D179, D180,
# MAGIC D181, D182, D183, D184, D185, D186, D187, D188, D189, D190,
# MAGIC D191, D192, D193, D194, D195, D196, D197, D198, D199, D200,
# MAGIC D201, D202, D203, D204, D205, D206, D207, D208, D209, D210,
# MAGIC D211, D212, D213, D214, D215, D216, D217, D218, D219, D220,
# MAGIC D221, D222, D223, D224, D225, D226, D227, D228, D229, D230,
# MAGIC D231, D232, D233, D234, D235, D236, D237, D238, D239, D240,
# MAGIC D241, D242, D243, D244, D245, D246, D247, D248, D249, D250,
# MAGIC D251, D252, D253, D254, D255, D256, D257, D258, D259, D260,
# MAGIC D261, D262, D263, D264, D265, D266, D267, D268, D269, D270,
# MAGIC D271, D272, D273, D274, D275, D276, D277, D278, D279, D280,
# MAGIC D281, D282, D283, D284, D285, D286, D287, D288, D289, D290,
# MAGIC D291, D292, D293, D294, D295, D296, D297, D298, D299, D300,
# MAGIC D301, D302, D303, D304, D305, D306, D307, D308, D309, D310,
# MAGIC D311, D312, D313, D314, D315, D316, D317, D318, D319, D320,
# MAGIC D321, D322, D323, D324, D325, D326, D327, D328, D329, D330,
# MAGIC D331, D332, D333, D334, D335, D336, D337, D338, D339, D340,
# MAGIC D341, D342, D343, D344, D345, D346, D347, D348, D349, D350,
# MAGIC D351, D352, D353, D354, D355, D356, D357, D358, D359, D360,
# MAGIC D361, D362, D363, D364, D365, D366)
# MAGIC )
# MAGIC JOIN acct_lookup_table a ON gl.ACCT = a.ACCT
# MAGIC WHERE a.ABReport_Det IN ('AFS - RPACE - PFG', 'HTM - CPACE', 'HTM - RPACE - PFG', 'HTM - RPACE')
# MAGIC ORDER BY ACCT_Number),
# MAGIC
# MAGIC /* To save Tableau processing time certain fields are precalculated  */
# MAGIC precomputation AS (
# MAGIC   SELECT *, 
# MAGIC   MAX(CASE WHEN Value > 0 THEN Date ELSE NULL END) OVER (PARTITION BY EntryMonth) AS Max_Date_With_Value,
# MAGIC   CONCAT(Pace_Type, ' - ', Maturity_Type) AS Maturity_Type_Header,
# MAGIC   (CASE WHEN Pace_Type = 'CPACE' THEN VAlue ELSE NULL END) AS CPACE_Value
# MAGIC FROM pivoted)
# MAGIC
# MAGIC
# MAGIC SELECT *
# MAGIC FROM precomputation;
