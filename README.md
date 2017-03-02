# tom
database
select * from tom_temp
drop table tom_temp purge;
create table tom_temp(nbr number,service_nbr varchar2(20));
select * from tom_temp for update;
alter table tom_temp add(prd_inst_id number);
update  tom_temp
set prd_inst_id=bdw.prd_crmdsg.prd_inst_id(service_nbr);
commit;

select * from tom_temp for update;
alter table tom_temp drop column COMPLETED_DATE; 
alter table tom_temp drop column ACTION; 
alter table tom_temp drop column 停机动作;

alter table tom_temp add(ACTION VARCHAR2(5), 停机动作 VARCHAR2(20), 停机时间 DATE);
merge into tom_temp a
using (select SERV_ID, ACTION, 停机动作, COMPLETED_DATE
         from (select SERV_ID,
                      t.ACTION,
                      decode(t.ACTION,21, '非实名双停', 22, '非实名单停') 停机动作,
                      t.COMPLETED_DATE,
                      row_number() over(partition by SERV_ID order by t.created_date desc nulls last) xh
                 from  hefei.TB_BIL_WORK_ORDER_551 t
                 where t.ACTION in (21,22))
        where xh = 1) b
on (a.prd_inst_id = b.SERV_ID)
when matched then
  update
     set a.ACTION   = b.ACTION,
         a.停机动作 = b.停机动作,
         a.停机时间 = b.COMPLETED_DATE;
 commit;  
 select * from hefei.TB_BIL_WORK_ORDER_551 where ACTION=21
 
 
drop table tom_temp4;
CREATE INDEX ID_tom_01_TOCH ON tom_temp(prd_inst_id);
create table tom_temp4
as
select a.prd_inst_id,prd_inst_stas_id
from bdw.mv_b_ft_prd_inst_day_551 a
where  exists(select 1 from tom_temp where prd_inst_id=a.prd_inst_id);



CREATE INDEX ID_tom_02_TOCH ON tom_temp4(prd_inst_id);
alter table tom_temp add(prd_inst_stas_id number);
update tom_temp a
set (prd_inst_stas_id)=
(select prd_inst_stas_id
  from tom_temp4 where  prd_inst_id=a.prd_inst_id);
commit;

select  decode(prd_inst_stas_id,
              100000,
              '正常',
              110000,
              '拆机',
              130000,
              '预实例化',
              110098,
              '注销',
              120000,
              '申请停机',
              120002,
              '双停',
              120005,
              '单停',
              122001,
              '挂失状态',
              '67',
              '非实名制单停',
              '68',
              '非实名制双停',
              '63',
              '公安停机') 号码状态
, a.* from tom_temp a where a.prd_inst_stas_id in (67,68);
