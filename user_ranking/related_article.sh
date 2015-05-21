#mysql --database weee_comm -e"drop table weee_comm.guoxian_t1"
mysql --database weee_comm -e" create table weee_comm.guoxian_t1 as
select a.article_id, a.related_id, a.rec_create_time, a.rank_popular as related_rank
from
(select article_id, related_id, rec_create_time,
@article_rank := @article_rank + 0.0001 AS rank_popular
from
(select t1.id article_id, t2.id related_id, t2.rank_popular, t2.rec_create_time 
from weee_comm.article t1 join weee_comm.article t2 where t1.rec_creator_id=t2.rec_creator_id and t1.id<>t2.id  and t2.view_count>=30 and t2.publish='Y' order by t1.id asc, t2.rank_popular asc, t2.rec_create_time desc) b, (select @article_rank := -20000) r
order by b.article_id, b.rank_popular
) a
order by a.article_id
;"

#mysql --database weee_comm -e" drop table weee_comm.guoxian_t11"
mysql --database weee_comm -e" create table weee_comm.guoxian_t11 as
select t1.article_id,t1.related_id, t1.related_rank from weee_comm.guoxian_t1 t1 join (select article_id, min(related_rank) as min_rank from weee_comm.guoxian_t1 group by article_id) t2 on t1.article_id=t2.article_id and t1.related_rank<=t2.min_rank+0.0001 order by t1.article_id;"

#mysql --database weee_comm -e"drop table weee_comm.guoxian_t2"
mysql --database weee_comm -e" create table weee_comm.guoxian_t2 as 
select a.article_id, a.related_id, a.rec_create_time, a.rank_popular as related_rank
from
(select article_id, related_id, rec_create_time,
@article_rank2 := @article_rank2 + 0.0001 AS rank_popular
from
(select t1.id article_id, t2.id related_id, t2.rec_create_time from weee_comm.article t1 join weee_comm.article t2 where  t1.id<>t2.id and t2.type='original'  and t2.view_count>=30 and t2.publish='Y' order by t2.rec_create_time desc) b, (select @article_rank2 := -10000) r
order by b.article_id, b.rec_create_time desc
) a
where a.rank_popular<=-1
order by a.article_id;"

#mysql --database weee_comm -e"drop table weee_comm.guoxian_t21"
mysql --database weee_comm -e"create table weee_comm.guoxian_t21 as
select t1.article_id,t1.related_id, t1.related_rank from weee_comm.guoxian_t2 t1 join (select article_id, min(related_rank) as min_rank from weee_comm.guoxian_t2 group by article_id) t2 on t1.article_id=t2.article_id and t1.related_rank<=t2.min_rank+0.0001 order by t1.article_id;"

#mysql --database weee_comm -e"drop table weee_comm.guoxian_t31"
mysql --database weee_comm -e" create table weee_comm.guoxian_t31 as
select a.article_id, a.related_id, a.rec_create_time, a.rank_popular as related_rank
from
(select t1.id article_id, t2.id related_id, t2.rec_create_time,  t2.rank_popular  from weee_comm.article t1 join weee_comm.article t2 where  t1.id<>t2.id and t2.view_count>=30 and t2.publish='Y' order by t2.rank_popular) a
where a.rank_popular<=200
order by a.article_id;"

#mysql --database weee_comm -e"drop table weee_comm.guoxian_t41"
mysql --database weee_comm -e"create table weee_comm.guoxian_t41 as
select -1 article_id, t2.id related_id, t2.rec_create_time,  t2.rank_popular as related_rank  from weee_comm.article t2 where t2.rank_popular<=200 and t2.view_count>=30 and t2.publish='Y';"

#mysql --database weee_comm -e"drop table weee_comm.guoxian_t5"
mysql --database weee_comm -e"create table weee_comm.guoxian_t5 as
select article_id, related_id, min(related_rank) as related_rank from
((select article_id, related_id, related_rank from weee_comm.guoxian_t11) 
union
(select article_id, related_id, related_rank from weee_comm.guoxian_t21) 
union
(select article_id, related_id, related_rank from weee_comm.guoxian_t31)
union
(select article_id, related_id, related_rank from weee_comm.guoxian_t41)) t1
group by t1.article_id,t1.related_id;"

#mysql --database weee_comm -e"drop table weee_comm.guoxian_t50"
mysql --database weee_comm -e"create table weee_comm.guoxian_t50 as
select t5.article_id, t5.related_id, 
@related_rank := @related_rank + 1 as related_rank
from weee_comm.guoxian_t5 t5, (select @related_rank:=1) r
order by t5.article_id,t5.related_rank;"
 
#mysql --database weee_comm -e"drop table weee_comm.guoxian_t51;"
mysql --database weee_comm -e"create table weee_comm.guoxian_t51 as
select t1.article_id,t1.related_id, t1.related_rank-t2.min_rank+1 as related_rank from weee_comm.guoxian_t50 t1 join (select article_id, min(related_rank) as min_rank from weee_comm.guoxian_t50 group by article_id) t2 on t1.article_id=t2.article_id and t1.related_rank<=t2.min_rank+4 order by t1.article_id;"

mysql --database weee_comm -e"
insert into weee_comm.related_article(article_id,r1,r2,r3,r4,r5)
select article_id, 
max(coalesce(case when related_rank=1 then related_id end,0)) as r1,
max(coalesce(case when related_rank=2 then related_id end,0)) as r2,
max(coalesce(case when related_rank=3 then related_id end,0)) as r3,
max(coalesce(case when related_rank=4 then related_id end,0)) as r4,
max(coalesce(case when related_rank=5 then related_id end,0)) as r5
from 
weee_comm.guoxian_t51
group by article_id
on duplicate key update r1=values(r1), r2=values(r2), r3=values(r3),r4=values(r4),r5=values(r5);"

mysql --database weee_comm -e"drop table weee_comm.guoxian_t1;"
mysql --database weee_comm -e"drop table weee_comm.guoxian_t11;"
mysql --database weee_comm -e"drop table weee_comm.guoxian_t2;"
mysql --database weee_comm -e"drop table weee_comm.guoxian_t21;"
mysql --database weee_comm -e"drop table weee_comm.guoxian_t31"
mysql --database weee_comm -e"drop table weee_comm.guoxian_t41;"
mysql --database weee_comm -e"drop table weee_comm.guoxian_t5;"
mysql --database weee_comm -e"drop table weee_comm.guoxian_t50;"
mysql --database weee_comm -e"drop table weee_comm.guoxian_t51;"
