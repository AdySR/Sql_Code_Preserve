
set ansi_nulls on
go
set quoted_identifier on
go
alter proc [edw_dr].[source_to_target_dr_recon] @targettablename [varchar](200),@keycolumn [varchar](200)
--,@reconaudit char 

as

/*************************************************************************************************************************************************

procedure name                  :     [source_to_target_dr]   

create date / modify date       :     27 june 2023

author                          :     aditya.r     

description                     :     this procedure is used to perform insert,update and delete operations based on staging table to the actual table

call by                         :     data_replication_pipeline[azure synapse analytics]

parameters(s)                   :     @targettablename - insert, update and delete operations will be performed on this table

.........  @keycolumn - this is used in where condition to perform update and delete operations

                                
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
declare @reconaudit char = 'y';
--declare @targettablename [varchar](200) ='vbrp'
--declare @keycolumn [varchar](200) ='MANDT, VBELN, POSNR'
declare @load_date datetime = getdate()
declare @updated_date datetime = @load_date
declare @sourceuniqueinsertcountstring nvarchar(max)=''
declare @sourceuniqueinsertcount int
declare @sourceuniquedeletecountstring nvarchar(max)=''
declare @sourceuniquedeletecount int
declare @sourcecolumn nvarchar(max)=''
declare @deletescriptcountstring nvarchar(max)=''
declare @deletescriptcount int;
declare @deletescriptstring nvarchar(max)=''
declare @insertscriptstring nvarchar(max)=''
declare @insertscriptcountstring nvarchar(max)=''
declare @insertscriptcount int;
declare @deletecommonstring nvarchar(max)=''
declare @deletecommoncount int;
declare @oncondition nvarchar(max)='';
declare @uniqueinsertquery nvarchar(max)='';
declare @uniquedeletequery nvarchar(max)='';
declare @deletecommoninsertstring nvarchar(max);
declare @deletecommondeletestring nvarchar(max);
declare @source_insert_del_in_tandem_countstring nvarchar(max)='';
declare @sourceinsertdeleteintandemcount int;
declare @uniqueupdatequery nvarchar(max)='';
declare @uniqueupdatequerycount int;
declare @uniqueupdatequerycountstring nvarchar(max)='';
declare @deletecommon_insert_delete_string nvarchar(max)=''
declare @deletecommon_update_delete_string nvarchar(max)=''
declare @sourceupdatedeleteintandemcountstring nvarchar(max)=''
declare @sourceupdatedeleteintandemcount int;
declare @deletecommonupdatestring nvarchar(max)='';
declare @update_script_string nvarchar(max)='';
declare @update_script_count_string nvarchar(max)='';
declare @update_script_count int;
declare @totalsourcecountstring nvarchar(max)='';
declare @totalsourcecount int;
declare @totalsourcedeletestring nvarchar(max)='';
declare @totalsourcedeletecount int;
declare @totalsourceupdatestring nvarchar(max)='';
declare @totalsourceupdatecount int;
declare @totalsourceinsertstring nvarchar(max)='';
declare @totalsourceinsertcount int;
declare @targetbeforeloadcountstring nvarchar(max)='';
declare @targetbeforeloadcount int;
declare @targetafterloadcountstring nvarchar(max)='';
declare @targetafterloadcount int;
declare @inserts_already_existing_string nvarchar(max)='';
declare @inserts_already_existing_count int;
declare @deletes_not_existing_string nvarchar(max)='';
declare @deletes_not_existing_count int;

if upper(@reconaudit)='y'
begin


begin try




set @targetafterloadcountstring = N' select @targetafterloadcount = count(1) from src_sap_r3.'+@targettablename
exec sp_executesql @targetafterloadcountstring, N'@targetafterloadcount int output', @targetafterloadcount output


set @tabledetails = concat('src_sap_r3.',@targettablename)
set @stgtabledetails = concat('stg_sap_r3.','stg_',@targettablename)

if object_id(N'tempdb..#columnnames',N'U') is not null
drop table #columnnames
if object_id(N'tempdb..#columnnames1',N'U') is not null
drop table #columnnames1


create table #columnnames with(distribution= hash(column_name)) as 
select row_number() over (order by column_name) as rownum,column_name
from (select column_name from information_schema.columns where table_schema = 'src_sap_r3' and table_name = @targettablename and column_name not in('load_date','updated_date'))a


set @total = (select count(*) from #columnnames)
create table #columnnames1 with(distribution=hash(rownum)) as 
select row_number() over(order by value) as rownum, value
from (select value from string_split(@keycolumn,','))a
set @total1 = (select count(*) from #columnnames1)

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



set @targetbeforeloadcountstring = N' select @targetbeforeloadcount = count(1) from src_sap_r3.'+@targettablename
print 'query_1#  ' + @targetbeforeloadcountstring
exec sp_executesql @targetbeforeloadcountstring, N'@targetbeforeloadcount int output', @targetbeforeloadcount output



set @totalsourcecountstring=N' select @totalsourcecount = count(1) from stg_sap_r3.stg_'+@targettablename 
print 'query_2#  ' + @totalsourcecountstring
exec sp_executesql @totalsourcecountstring, N'@totalsourcecount int output', @totalsourcecount output


set @totalsourceinsertstring=N' select @totalsourceinsertcount = count(1) from stg_sap_r3.stg_'+@targettablename+' where operationondata =2'
print 'query_3#  ' + @totalsourceinsertstring
exec sp_executesql @totalsourceinsertstring, N'@totalsourceinsertcount int output', @totalsourceinsertcount output


set @totalsourcedeletestring=N' select @totalsourcedeletecount = count(1) from stg_sap_r3.stg_'+@targettablename+' where operationondata =1'
print 'query_4#  ' + @totalsourcedeletestring
exec sp_executesql @totalsourcedeletestring, N'@totalsourcedeletecount int output', @totalsourcedeletecount output


set @totalsourceupdatestring=N' select @totalsourceupdatecount = count(1) from stg_sap_r3.stg_'+@targettablename+' where operationondata =4'
print 'query_5#  ' + @totalsourceupdatestring
exec sp_executesql @totalsourceupdatestring, N'@totalsourceupdatecount int output', @totalsourceupdatecount output

select @totalsourcecount as '@totalsourcecount' , @totalsourceinsertcount as '@totalsourceinsertcount' , @totalsourcedeletecount as '@totalsourcedeletecount' ,@totalsourceupdatecount as '@totalsourceupdatecount'


set @uniqueinsertquery ='
select '+@columnstring+'
into #uniqueinsert_'+@targettablename+'
from(
select row_number() over(partition by '+@keycolumn+' order by __$start_lsn desc) rn ,* from stg_sap_r3.stg_'+@targettablename+' where operationondata =2)t
where rn=1'


set @uniquedeletequery ='
select '+@columnstring+' 
into #uniquedelete_'+@targettablename+'
from(
select row_number() over(partition by '+@keycolumn+' order by __$start_lsn desc) rn ,* from stg_sap_r3.stg_'+@targettablename+' where operationondata =1)t
where rn=1'


set @uniqueupdatequery='
select '+@columnstring+' 
into #uniqueupdate_'+@targettablename+'
from(
select row_number() over (partition by  '+@keycolumn+' order by __$start_lsn desc) rn ,* from stg_sap_r3.stg_'+@targettablename+' where operationondata =4)t
where rn=1'

print 'query_6#' +@uniqueinsertquery
print 'query_7#' +@uniquedeletequery
print 'query_8#' +@uniqueupdatequery

exec sp_executesql @uniqueinsertquery
exec sp_executesql @uniquedeletequery
exec sp_executesql @uniqueupdatequery

set @sourceuniqueinsertcountstring = N'select @sourceuniqueinsertcount =count(1) from #uniqueinsert_'+@targettablename
print 'query_9#  ' +@sourceuniqueinsertcountstring
exec sp_executesql @sourceuniqueinsertcountstring, N'@sourceuniqueinsertcount int output', @sourceuniqueinsertcount output

set @sourceuniquedeletecountstring = N'select @sourceuniquedeletecount =count(1) from #uniquedelete_'+@targettablename
print 'query_10#  ' +@sourceuniquedeletecountstring
exec sp_executesql @sourceuniquedeletecountstring, N'@sourceuniquedeletecount int output', @sourceuniquedeletecount output

set @uniqueupdatequerycountstring = N'select @uniqueupdatequerycount =count(1) from #uniqueupdate_'+@targettablename
print 'query_11#  ' +@uniqueupdatequerycountstring
exec sp_executesql @uniqueupdatequerycountstring, N'@uniqueupdatequerycount int output', @uniqueupdatequerycount output

select @uniqueupdatequerycount as '@uniqueupdatequerycount', @sourceuniqueinsertcount as '@sourceuniqueinsertcount', @sourceuniquedeletecount as '@sourceuniquedeletecount'




/* handle scenario of insert and deletions in tandem */

set @deletecommon_insert_delete_string ='select t.* into #deletecommon_insert_delete_'+@targettablename+'  from #uniqueinsert_'+@targettablename+' t inner join #uniquedelete_'+@targettablename+' s on '+@whereconcatstringupdated
print  'query_12#  ' +@deletecommon_insert_delete_string
exec sp_executesql @deletecommon_insert_delete_string

set @deletecommon_update_delete_string ='select t.* into #deletecommon_update_delete_'+@targettablename+'  from #uniqueupdate_'+@targettablename+' t inner join #uniquedelete_'+@targettablename+' s on '+@whereconcatstringupdated
print  'query_13#  ' +@deletecommon_update_delete_string
exec sp_executesql @deletecommon_update_delete_string


set @source_insert_del_in_tandem_countstring = N'select @sourceinsertdeleteintandemcount = count(1) from  #deletecommon_insert_delete_'+@targettablename
print  'query_14#  ' +@source_insert_del_in_tandem_countstring
exec sp_executesql @source_insert_del_in_tandem_countstring, N'@sourceinsertdeleteintandemcount int output', @sourceinsertdeleteintandemcount output


set @sourceupdatedeleteintandemcountstring = N'select @sourceupdatedeleteintandemcount = count(1) from  #deletecommon_update_delete_'+@targettablename
print  'query_15#  ' +@sourceupdatedeleteintandemcountstring
exec sp_executesql @sourceupdatedeleteintandemcountstring, N'@sourceupdatedeleteintandemcount int output', @sourceupdatedeleteintandemcount output


select @sourceinsertdeleteintandemcount as '@sourceinsertdeleteintandemcount' , @sourceupdatedeleteintandemcount as '@sourceupdatedeleteintandemcount'


set @deletecommoninsertstring ='delete t from #uniqueinsert_'+@targettablename+' t inner join #deletecommon_insert_delete_'+@targettablename+' s on '+@whereconcatstringupdated
print  'query_16#  ' +@deletecommoninsertstring
exec sp_executesql @deletecommoninsertstring
set @deletecommonupdatestring ='delete t from #uniqueupdate_'+@targettablename+' t inner join #deletecommon_update_delete_'+@targettablename+' s on '+@whereconcatstringupdated
print  'query_17#  ' + @deletecommonupdatestring
exec sp_executesql @deletecommonupdatestring


set @deletecommondeletestring ='delete t from #uniquedelete_'+@targettablename+' t inner join #deletecommon_insert_delete_'+@targettablename+' s on '+@whereconcatstringupdated
print  'query_18#  ' +  @deletecommondeletestring
exec sp_executesql @deletecommondeletestring
set @deletecommondeletestring ='delete t from #uniquedelete_'+@targettablename+' t inner join #deletecommon_update_delete_'+@targettablename+' s on '+@whereconcatstringupdated
print  'query_19#  ' +  @deletecommondeletestring
exec sp_executesql @deletecommondeletestring


/* now we have clean data for dr 
now we will 
1. insert non existing inserts and catch the count
2. delete the existing records and catch the count
*/


/*	execute inserts	*/
--,InsertsAlreadyExisting
--,DeletesNotExisting



set @inserts_already_existing_string =' select @inserts_already_existing_count = count(1)  
from #uniqueinsert_'+@targettablename+' s inner join '+@tabledetails+' t	 on '+@whereconcatstringupdated
print  'query_20#  ' + @inserts_already_existing_string
exec sp_executesql @inserts_already_existing_string,N'@inserts_already_existing_count int output', @inserts_already_existing_count output

set @insertscriptcountstring ='select @insertscriptcount = count(1)
from #uniqueinsert_'+@targettablename+' s left join '+@tabledetails+' t	 on '+@whereconcatstringupdated+'
where '+@oncondition
print  'query_21#  ' +@insertscriptcountstring
exec sp_executesql @insertscriptcountstring,N'@insertscriptcount int output', @insertscriptcount output


set @insertscriptstring ='
insert into '+@tabledetails+' ( '+@columnstring+' , load_date , updated_date ) 
select '+@sourcecolumn+' '''+ convert(varchar, @load_date, 121) +''' , '''+  convert(varchar, @updated_date, 121) +''' 
from #uniqueinsert_'+@targettablename+' s left join '+@tabledetails+' t	 on '+@whereconcatstringupdated+'
where '+@oncondition
print  'query_22#  ' +@insertscriptstring
exec sp_executesql @insertscriptstring

/* execute updates */


set @update_script_count_string=N'
Select @update_script_count = count(1)  
from #uniqueupdate_'+@targettablename+' s inner join '+@tabledetails+' t	 on '+@whereconcatstringupdated
exec sp_executesql @update_script_count_string, N'@update_script_count int output', @update_script_count output


set @update_script_string=N'
update  t
set  ' + @updatedstringnew + ', updated_date = '''+  convert(varchar, @updated_date, 121) +''' 	
from	src_sap_r3.'+@targettablename+' t inner join #uniqueupdate_'+@targettablename+' s on '+@whereconcatstringupdated
print  'query_23#  ' +@update_script_string
exec sp_executesql @update_script_string

/*	execute deletion */

set @deletes_not_existing_string ='select @deletes_not_existing_count = count(1)
from #uniquedelete_'+@targettablename+' s left join '+@tabledetails+' t	 on '+@whereconcatstringupdated+'
where '+@oncondition
print  'query_24#  ' +@deletes_not_existing_string
exec sp_executesql @deletes_not_existing_string,N'@deletes_not_existing_count int output', @deletes_not_existing_count output

set @deletescriptcountstring ='select @deletescriptcount = count(1)
from #uniquedelete_'+@targettablename+' s inner join '+@tabledetails+' t	 on '+@whereconcatstringupdated
print  'query_25#  ' +@deletescriptcountstring
exec sp_executesql @deletescriptcountstring, N'@deletescriptcount int output', @deletescriptcount output

set @deletescriptstring='
delete t from '+@tabledetails+' t inner join #uniquedelete_'+@targettablename+' s on '+@whereconcatstringupdated
print  'query_26#  ' +@deletescriptstring
exec sp_executesql @deletescriptstring

select @deletescriptcount as '@deletescriptcount' , @insertscriptcount as '@insertscriptcount' , @update_script_count as '@update_script_count'

set @targetafterloadcountstring = N' select @targetafterloadcount = count(1) from src_sap_r3.'+@targettablename
print  'query_27#  ' +@targetafterloadcountstring
exec sp_executesql @targetafterloadcountstring, N'@targetafterloadcount int output', @targetafterloadcount output



insert into [edw_CTRL].[CDW_DR_Job_Recon_Details] (
DRPipelineRunDate
,TableName
,TotalSourceCount
,TotalSourceInsertCount
,TotalSourceUpdateCount
,TotalSourceDeleteCount
,SourceUniqueInsertCount
,SourceUniqueDeleteount
,SourceUniqueUpdateCount
,InsertsAlreadyExisting
,DeletesNotExisting
,InsertedCount
,DeletedCount
,UpdatedCount
,TargetBeforeLoadCount
,TargetAfterLoadCount
,SourceInsertDeleteInTandemCount
,SourceUpdateDeleteInTandemCount
,TargetDupesCount
)

Select Getdate(), 
@targettablename, 
@totalsourcecount, 
@totalsourceinsertcount, 
@totalsourceupdatecount, 
@totalsourcedeletecount, 
@sourceuniqueinsertcount, 
@sourceuniquedeletecount, 
@uniqueupdatequerycount,
@inserts_already_existing_count,
@deletes_not_existing_count,
@insertscriptcount,
@deletescriptcount,
@update_script_count,
@targetbeforeloadcount,
@targetafterloadcount,
@sourceinsertdeleteintandemcount,
@sourceupdatedeleteintandemcount,
null

end try

begin catch

			throw

end catch

end

else

begin

			select 1 


end

end

