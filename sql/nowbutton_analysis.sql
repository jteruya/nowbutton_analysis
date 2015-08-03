drop table if exists jt.nowbutton_actions;

--============================================================================================================
-- Table: jt.nowbutton_actions
-- Description: Get all the nowbutton actions from the fact_actions table.
--============================================================================================================

create table jt.nowbutton_actions as
select batch_id
     , row_id
     , tinserted
     , created
     , bundle_id
     , application_id
     , global_user_id
     , is_anonymous
     , lower(device_id) as device_id
     , app_type_id
     , device_os_version
     , binary_version
     , mmm_info
     , identifier
     , metadata
from fact_actions
where identifier = 'nowbutton' or identifier = 'nowButton';


drop table if exists jt.nowbutton_user_sessions;

--============================================================================================================
-- Table: jt.nowbutton_user_sessions
-- Description: Get all users with at least one app session for events that had at least one nowbutton action.
--============================================================================================================

create table jt.nowbutton_user_sessions as
select distinct lower(a.bundleid) as bundle_id
     , s.application_id
     , a.name as event_name
     , a.eventtype
     , a.startdate
     , a.enddate
     , s.user_id
     , s.app_type_id
     , lower(u.global_user_id) as global_user_id
from (select * from fact_sessions where application_id in (select distinct application_id from jt.nowbutton_actions)) s
join (select lower(applicationid) as application_id
           , userid
           , lower(globaluserid) as global_user_id
      from authdb_is_users where applicationid in (select distinct upper(application_id) from jt.nowbutton_actions)) u
on s.application_id = u.application_id and s.user_id = u.userid
join authdb_applications a
on u.application_id = lower(a.applicationid);


drop table if exists jt.nowbutton_agenda_view;

--============================================================================================================
-- Table: jt.nowbutton_user_sessions
-- Description: A subset of the users in jt.nowbutton_user_sessions who has at least one agenda view.
--============================================================================================================

create table jt.nowbutton_agenda_view as
select distinct lower(a.bundleid) as bundle_id
     , v.application_id
     , a.name as event_name
     , a.eventtype
     , a.startdate
     , a.enddate
     , u.userid as user_id
     , v.app_type_id
     , lower(v.global_user_id) as global_user_id
from (select * from fact_views where identifier = 'list' and metadata->>'type' = 'agenda' and application_id in (select distinct application_id from jt.nowbutton_actions)) v
join (select lower(applicationid) as application_id
           , userid
           , lower(globaluserid) as global_user_id
      from authdb_is_users where applicationid in (select distinct upper(application_id) from jt.nowbutton_actions)) u
on v.application_id = u.application_id and v.global_user_id = u.global_user_id
join authdb_applications a
on u.application_id = lower(a.applicationid);


drop table if exists jt.nowbutton_testevents;

--============================================================================================================
-- Table: jt.nowbutton_testevents
-- Description: Identify the Test Events through two methods:

-- 1a. Identify if the naming of the Event has anything to do with a DoubleDutch test/internal/QA Event
-- 1b. Identify if the specific Bundle Unique ID is tied to a test event (as specified by internal users)
-- 2.  Check if the Event has 20 or fewer Users across all Event sessions (or no Event sessions at all)
--============================================================================================================

create table jt.nowbutton_testevents as
select s.*
from (select distinct lower(applicationid) as application_id
           , trim(a.name) as name
      from public.authdb_applications a
      join public.authdb_bundles b on a.bundleid = b.bundleid
      
      -- 1a --
      where lower(a.name) like '%doubledutch%'
      or lower(b.name) like '%doubledutch%'
      or lower(b.name) in ('pride','ddqa')

      -- 1b --
      or a.bundleid in ('00000000-0000-0000-0000-000000000000','025aa15b-ce74-40aa-a4cc-04028401c8b3','89fd8f03-0d59-41ab-a6a7-2237d8ac4eb2','5a46600a-156a-441e-b594-40f7defb54f2','f95fe4a7-e86a-4661-ac59-8b423f1f540a','34b4e501-3f31-46a0-8f2a-0fb6ea5e4357','09e25995-8d8f-4c2d-8f55-15ba22595e11','5637be65-6e3f-4095-beb8-115849b5584a','9f3489d7-c93c-4c8b-8603-dda6a9061116','d0f56154-e8e7-4566-a845-d3f47b8b35cc','bc35d4ce-c571-4f91-834a-a8136ca137c4','3e3fda3d-a606-4013-8ddf-711a1871bd12','75ce91a5-bcc0-459a-b479-b3956ea09abc','384d052e-0abd-44d1-a643-bc590135f5a0','b752a5b3-aa53-4bcf-9f52-d5600474d198','15740a5a-25d8-4dc6-a9ed-7f610ff94085','0cbc9d00-1e6d-4db3-95fc-c5fbb156c6de','f0c4b2db-a743-4fb2-9e8f-a80463e52b55','8a995a58-c574-421b-8f82-e3425d9054b0','6dbb91c8-6544-48ef-8b8d-a01b435f3757','f21325d8-3a43-4275-a8b8-b4b6e3f62de0','de8d1832-b4ea-4bd2-ab4b-732321328b04','7e289a59-e573-454c-825b-cf31b74c8506')
   
      union

      -- 2 --
      select lower(a.applicationid) as application_id
           , trim(a.name) as name
      from (select * from public.authdb_applications where lower(applicationid) in (select distinct application_id from jt.nowbutton_actions)) a
      left join (select distinct application_id, user_id from jt.nowbutton_user_sessions) s 
      on lower(a.applicationid) = s.application_id
      group by 1,2
      having count(*) <= 20) s;


drop table if exists jt.nowbutton_actions_all_users;

--============================================================================================================
-- Table: jt.nowbutton_actions_all_users
-- Description: This combines:
--      (1) Users with at least one app session.
--      (2) Users with at least one agenda view.
--      (3) Users with at least one nowbutton action.
--============================================================================================================

create table jt.nowbutton_actions_all_users as
select u.bundle_id
     , u.application_id
     , case
         when t.application_id is null then 1
         else 0
         end as nontestevent
     , u.event_name
     , u.eventtype
     , u.startdate
     , u.enddate
     , u.user_id
     , u.global_user_id
     , u.app_type_id as os_session_app_type_id
     , v.app_type_id as os_agenda_app_type_id
     , case
         when v.application_id is not null then 1
         else 0
         end as view_agenda_flag
     , f.created
     , f.is_anonymous
     , f.device_id
     , f.app_type_id as os_nowbutton_app_type_id
     , f.device_os_version
     , f.binary_version
     , f.mmm_info
     , f.identifier
     , f.metadata
from jt.nowbutton_user_sessions u
left join jt.nowbutton_agenda_view v
on u.application_id = v.application_id and u.global_user_id = v.global_user_id
left join jt.nowbutton_actions f
on f.application_id = u.application_id and f.global_user_id = u.global_user_id
left join jt.nowbutton_testevents t
on t.application_id = u.application_id;

drop table if exists jt.nowbutton_summary_stats;

--============================================================================================================
-- Table: jt.nowbutton_summary_stats
-- Description: Summary Stats per event on now button
--============================================================================================================

create table jt.nowbutton_summary_stats as
select bundle_id
     , application_id
     , nontestevent
     , event_name
     , startdate
     , enddate
     , count(*) as app_user_cnt
     , count(case when agenda_view_cnt > 0 then 1 else null end) as agenda_user_cnt
     , count(case when nowbutton_action_cnt >= 1 then 1 else null end) as nowbutton_user_cnt
     , count(case when nowbutton_action_cnt > 1 then 1 else null end) as nowbutton_repeat_user_cnt
     , count(case when agenda_view_cnt > 0 then 1 else null end)::decimal(12,4)/count(*)::decimal(12,4) as agenda_user_pct
     , count(case when nowbutton_action_cnt >= 1 then 1 else null end)::decimal(12,4)/count(case when agenda_view_cnt > 0 then 1 else null end)::decimal(12,4) as nowbutton_user_pct
     , count(case when nowbutton_action_cnt > 1 then 1 else null end)::decimal(12,4)/count(case when agenda_view_cnt > 0 then 1 else null end)::decimal(12,4) as nowbutton_repeat_user_pct
from (select bundle_id
           , application_id
           , nontestevent
           , event_name
           , startdate
           , enddate
           , global_user_id
           , user_id
           , os_session_app_type_id
           , os_agenda_app_type_id
           , os_nowbutton_app_type_id
           , count(case when view_agenda_flag = 1 then 1 else null end) as agenda_view_cnt         
           , count(case when created is not null then 1 else null end) as nowbutton_action_cnt
      from jt.nowbutton_actions_all_users
      where enddate < current_date
      and os_session_app_type_id in (1,2,3)
      group by 1,2,3,4,5,6,7,8,9,10,11) a
where nontestevent = 1
group by 1,2,3,4,5,6
order by 3,5,6;

drop table if exists jt.nowbutton_ios_summary_stats;

--============================================================================================================
-- Table: jt.nowbutton_ios_summary_stats
-- Description: Summary Stats per event on now button (iOS)
--============================================================================================================

create table jt.nowbutton_ios_summary_stats as 
select bundle_id
     , application_id
     , nontestevent
     , event_name
     , startdate
     , enddate
     , count(*) as app_user_cnt
     , count(case when agenda_view_cnt > 0 then 1 else null end) as agenda_ios_user_cnt
     , count(case when nowbutton_action_cnt >= 1 then 1 else null end) as nowbutton_ios_user_cnt
     , count(case when nowbutton_action_cnt > 1 then 1 else null end) as nowbutton_repeat_ios_user_cnt
     , count(case when agenda_view_cnt > 0 then 1 else null end)::decimal(12,4)/count(*)::decimal(12,4) as agenda_ios_user_pct
     , count(case when nowbutton_action_cnt >= 1 then 1 else null end)::decimal(12,4)/count(case when agenda_view_cnt > 0 then 1 else null end)::decimal(12,4) as nowbutton_ios_user_pct
     , count(case when nowbutton_action_cnt > 1 then 1 else null end)::decimal(12,4)/count(case when agenda_view_cnt > 0 then 1 else null end)::decimal(12,4) as nowbutton_repeat_ios_user_pct
from (select bundle_id
           , application_id
           , nontestevent
           , event_name
           , startdate
           , enddate
           , global_user_id
           , user_id
           , os_session_app_type_id
           , os_agenda_app_type_id
           , os_nowbutton_app_type_id
           , count(case when view_agenda_flag = 1 then 1 else null end) as agenda_view_cnt         
           , count(case when created is not null then 1 else null end) as nowbutton_action_cnt
      from jt.nowbutton_actions_all_users
      where enddate < current_date
      and os_session_app_type_id in (1,2)
      group by 1,2,3,4,5,6,7,8,9,10,11) a
where nontestevent = 1
group by 1,2,3,4,5,6
order by 3,5,6;


drop table if exists jt.nowbutton_android_summary_stats;

--============================================================================================================
-- Table: jt.nowbutton_android_summary_stats
-- Description: Summary Stats per event on now button (Android)
--============================================================================================================

create table jt.nowbutton_android_summary_stats as
select bundle_id
     , application_id
     , nontestevent
     , event_name
     , startdate
     , enddate
     , count(*) as app_user_cnt
     , count(case when agenda_view_cnt > 0 then 1 else null end) as agenda_android_user_cnt
     , count(case when nowbutton_action_cnt >= 1 then 1 else null end) as nowbutton_android_user_cnt
     , count(case when nowbutton_action_cnt > 1 then 1 else null end) as nowbutton_repeat_android_user_cnt
     , count(case when agenda_view_cnt > 0 then 1 else null end)::decimal(12,4)/count(*)::decimal(12,4) as agenda_android_user_pct
     , count(case when nowbutton_action_cnt >= 1 then 1 else null end)::decimal(12,4)/count(case when agenda_view_cnt > 0 then 1 else null end)::decimal(12,4) as nowbutton_android_user_pct
     , count(case when nowbutton_action_cnt > 1 then 1 else null end)::decimal(12,4)/count(case when agenda_view_cnt > 0 then 1 else null end)::decimal(12,4) as nowbutton_repeat_android_user_pct
from (select bundle_id
           , application_id
           , nontestevent
           , event_name
           , startdate
           , enddate
           , global_user_id
           , user_id
           , os_session_app_type_id
           , os_agenda_app_type_id
           , os_nowbutton_app_type_id
           , count(case when view_agenda_flag = 1 then 1 else null end) as agenda_view_cnt         
           , count(case when created is not null then 1 else null end) as nowbutton_action_cnt
      from jt.nowbutton_actions_all_users
      where enddate < current_date
      and os_session_app_type_id in (3)
      group by 1,2,3,4,5,6,7,8,9,10,11) a
where nontestevent = 1
group by 1,2,3,4,5,6
order by 3,5,6;

--============================================================================================================
-- Event Level Summary
--============================================================================================================
\copy (select * from jt.nowbutton_summary_stats) to '/home/jteruya/nowbutton_analysis/csv/event_level_summary.csv' with csv;
\copy (select * from jt.nowbutton_ios_summary_stats) to '/home/jteruya/nowbutton_analysis/csv/ios_event_level_summary.csv' with csv;
\copy (select * from jt.nowbutton_android_summary_stats) to '/home/jteruya/nowbutton_analysis/csv/android_event_level_summary.csv' with csv;
