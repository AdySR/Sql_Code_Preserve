Create PROC [edw_ctrl].[Src_TableSchemaLookup_Ingestion_Load] @targettablename [varchar](200),@keycolumn [varchar](200) AS


/*************************************************************************************************************************************************
procedure name                  :     [Src_TableSchemaLookup_INGESTION]
create date / modify date       :     22 Sep 2023
author                          :     Alka     
description                     :     this procedure is used to write CDC table details in Src_TableSchemaLookup
call by                         :     ADF
parameters(s)                   :     @targettablename [varchar](200),@keycolumn - Key column details will be put on this table

**************************************************************************************************************************************************/

begin
declare @tabledetails nvarchar(max);
declare @stgtabledetails nvarchar(max);
declare @updatedstring nvarchar(max);
declare @whereconcatstring nvarchar(max);
declare @whereconcatstringupdated nvarchar(max);
declare @executesqlupdate nvarchar(max);
declare @executesqlinsert nvarchar(max);
declare @executesqldelete nvarchar(max);
declare @updatedstringnew nvarchar(max);
declare @updaterecent nvarchar(max);
declare @inserttgtdistinctstring nvarchar(max);
declare @inserttgtdistinctcount int;
declare @total1 int;
declare @i int = 1;
declare @j int = 1;
declare @concat as varchar(max)='';
declare @concatnew as varchar(max)='';
declare @updateconcat as varchar(max)='';
declare @whereconcat as varchar(max)='';
declare @concatremovechar varchar(max);
declare @columnstring varchar(max);
declare @columnstringnew varchar(max);
declare @columnname varchar(max);
declare @total int;
declare @columnname1 varchar(max);
declare @load_date datetime = getdate()
declare @updated_date datetime = @load_date
declare @oncondition nvarchar(max)='';
declare @sourcecolumn nvarchar(max)=''
declare @record_count int ;
declare @sequence_number int = 1 ;
declare @operation_on_data_value int;
declare @insert_loop_string nvarchar(max)='';
declare @delete_loop_string nvarchar(max)='';
declare @ins_del_common_table_string nvarchar(max)='';
declare @inserts_string nvarchar(max)='';
declare @delete_string nvarchar(max)='';
declare @common_delete_ins_count int;
declare @unique_inserts_query nvarchar(max)='';
declare @unique_deletes_query nvarchar(max)='';
declare @unique_updates_query nvarchar(max)='';
declare @unique_inserts_qualified_query nvarchar(max)='';
declare @unique_deletes_qualified_query nvarchar(max)='';
declare @insert_script_query nvarchar(max)='';
declare @delete_script_query nvarchar(max)='';
declare @update_script_string nvarchar(max)='';
declare @total_source_insert_string nvarchar(max)='';
declare @total_source_insert_count int;
declare @total_source_delete_string nvarchar(max)='';
declare @total_source_delete_count int;
declare @total_source_update_string nvarchar(max)='';
declare @total_source_update_count int;
declare @unique_inserts_count_query nvarchar(max)='';
declare @unique_inserts_count nvarchar(max)='';
declare @unique_updates_count_query nvarchar(max)='';
declare @unique_updates_count nvarchar(max)='';
declare @unique_deletes_count_query nvarchar(max)='';
declare @unique_deletes_count nvarchar(max)='';
declare @target_before_load_count_string nvarchar(max)='';
declare @target_before_load_count int;
declare @total_source_count_string nvarchar(max)='';
declare @total_source_count int;
declare @in_tandem_inserts int;
declare @in_tandem_deletes int;
declare @reconaudit char = 'Y';
declare @ins_loop_count_string nvarchar(max)='';
declare @del_loop_count_string nvarchar(max)='';
declare @ins_loop_count int;
declare @del_loop_count int;
declare @inserts_already_existing_string nvarchar(max)='';
declare @inserts_already_existing_count int;
declare @orphan_update_count_string nvarchar(max)='';
declare @orphan_update_count int;
declare @deletes_not_existing_string nvarchar(max)='';
declare @deletes_not_existing_count int;
declare @target_after_load_count_string nvarchar(max)='';
declare @target_after_load_count int;
declare @insert_loop_counter int =0;
declare @delete_loop_counter int =0;
declare @delete_loop_if_exists_query nvarchar(max)='';
declare @insert_loop_if_exists_query nvarchar(max)='';
declare @insert_loop_if_exists_query_count int;
declare @delete_loop_if_exists_query_count int;
declare @ins_del_common_loop_lookup_query nvarchar(max)='';
declare @RankedInsDelList_Query nvarchar(max)=''
declare @PurgeOperationInTandem nvarchar(max)='';
declare @InsertOperationInTandem nvarchar(max)='';
declare @PurgeBadUpdateRecords nvarchar(max)=''

set @tabledetails = concat('src_sap_r3.',@targettablename)
set @stgtabledetails = concat('src_sap_r3.','stg_',@targettablename)


--begin try


if object_id(N'tempdb..#columnnames',N'U') is not null
drop table #columnnames
if object_id(N'tempdb..#columnnames1',N'U') is not null
drop table #columnnames1


create table #columnnames with(DISTRIBUTION = ROUND_ROBIN, HEAP) as 
select row_number() over (order by column_name) as rownum,column_name
from (select column_name from information_schema.columns where table_schema = 'src_sap_r3' and table_name = @targettablename and column_name not in('load_date'))a
set @total = (select count(*) from #columnnames)
create table #columnnames1 with(DISTRIBUTION = ROUND_ROBIN, HEAP) as 
select row_number() over(order by value) as rownum, value
from (select value from string_split(@keycolumn,','))a
set @total1 = (select count(1) from #columnnames1)


while(@i <= @total)
begin
	set @columnname = (select concat('[',column_name,']') from #columnnames where rownum = @i);
	set @concat += @columnname + ','
	set @concatnew += 's.' + @columnname + ','
	set @updateconcat += 't.' + @columnname + '= s.' + @columnname + ',';
	set @i+=1
end
while(@j <= @total1)
begin
	set @columnname1 = (select value from #columnnames1 where rownum = @j)
	set @whereconcat += ' t.' + @columnname1 +  ' = s.' + @columnname1 + ' and ';
	set @oncondition +=' t.' + @columnname1 + ' is null  and '
	set @j+=1
end
	set @columnstring  = left(@concat, len(@concat)-1);
	set @columnstringnew  = left(@concatnew, len(@concat)-1);
	set @updatedstring = replace(@updateconcat,@targettablename+'.','s');
	set @updatedstringnew = left(@updatedstring, len(@updatedstring)-1);
	set @sourcecolumn = replace(@concat, '[','s.[')

if object_id(N'tempdb..#columnnames1', N'U') is not null
begin
	set @whereconcatstring = replace(@whereconcat,@targettablename+'.', 's');
	set @whereconcatstringupdated = left(@whereconcatstring,len(@whereconcatstring)-3);
	set @oncondition = left(@oncondition,len(@oncondition)-3);
end
/*IF OBJECT_ID('tempdb.#Src_TableSchemaLookup_INGESTION') IS NOT NULL
		DROP TABLE tempdb.#Src_TableSchemaLookup_INGESTION;

CREATE TABLE tempdb.#Src_TableSchemaLookup_INGESTION WITH
(
            DISTRIBUTION = ROUND_ROBIN,
            HEAP
) AS*/


insert into  edw_ctrl.SRC_TableSchemaLookup (TableName,	KeyColumn	,TargetTableName	,StageTableName	,ColumnString	,WhereStringUpdate	,SourceColumn	,OnCondition	,UpdatedStringNew)
select @targettablename as targettablename,@keycolumn as keycolumn,@stgtabledetails as stgtabledetails,@columnstring as columnstring,@whereconcatstringupdated as whereconcatstringupdated,@tabledetails as tabledetails,@sourcecolumn as sourcecolumn,@oncondition as oncondition,@updatedstringnew as updatedstringnew;
/*select tabledetails,keycolumn,TargetTableName,stgtabledetails as StageTableName,ColumnString,whereconcatstringupdated as WhereStringUpdate,SourceColumn,OnCondition,UpdatedStringNew
from tempdb.#Src_TableSchemaLookup_INGESTION;*/

end