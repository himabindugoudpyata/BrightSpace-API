

select OrganizationalUnits.Name,
OrganizationalUnits.OrgUnitId,
OrganizationalUnits.Organization,
EnrollmentsAndWithdrawals.UserId,
EnrollmentsAndWithdrawals.RoleId,
OrganizationalUnits.Type,
EnrollmentsAndWithdrawals.EnrollmentDate
INTO newtable
FROM EnrollmentsAndWithdrawals 
INNER JOIN OrganizationalUnits 
ON 
EnrollmentsAndWithdrawals.OrgUnitId=OrganizationalUnits.OrgUnitId 
where  RoleId = '157' AND Type ='Course Offering' AND EnrollmentDate >= '2019-06-21'
    AND EnrollmentDate < '2021-03-22'

select UserId,Organization,OrgUnitId,Type,Name,EnrollmentDate into Query from newtable

select * from Query  Order by EnrollmentDate DESC


######## Table ---Query(main table)#############

UserId | Organization | OrgUnitId | TYpe | Name |  EnrollmentDate


--------------ASSIGNMENT sUBMITTED PER USER---------------


select 
AssignmentSubmissions.OrgUnitId,Query.UserId,AssignmentSubmissions.DropboxId,AssignmentSubmissions.FileSubmissionCount
into assignmentCountPerUser
from AssignmentSubmissions
INNER JOIN Query 
ON 
Query.OrgUnitId=AssignmentSubmissions.OrgUnitId AND Query.UserId =AssignmentSubmissions.SubmitterId

select top 20 * from assignmentCountPerUser

-------------Number of assignments submitted by a user-----

SELECT  OrgUnitId, UserId,COUNT(DISTINCT DropboxId) as DropboxIdCount
FROM assignmentCountPerUser
GROUP BY OrgUnitId,UserId

drop table assignmentCountPerUser 



############## Table (assignmentCountPerUser)#######

OrgUnitId | UserId |  DropboxIdCount

-------------Number of Unique quizes attempted by a user-----

select QuizAttempts.OrgUnitId,QuizAttempts.UserId,QuizAttempts.QuizId,QuizAttempts.AttemptNumber
into QuizAttemptsPerUser
from QuizAttempts
INNER JOIN
Query
ON Query.UserId=QuizAttempts.UserId AND Query.OrgUnitId = QuizAttempts.OrgUnitId

select top 20 * from QuizAttemptsPerUser



-------------Number of Unique quizes attempted by a user-----

SELECT  OrgUnitId, UserId,COUNT(DISTINCT QuizId) as QuizIdCount
FROM QuizAttemptsPerUser
GROUP BY OrgUnitId,UserId


###################Table   (QuizAttemptsPerUser)##############################

OrgUnitId | UserId | QuizIdCount





drop table QuizAttemptsPerUser


--------------------Number of Unique pots read by a user------------------------


select DiscussionPosts.topicid, DiscussionPosts.PostId, DiscussionPosts.UserId, DiscussionPosts.OrgUnitId ,DiscussionPostsReadStatus.IsRead
into Discussions
from DiscussionPosts
INNER JOIN
DiscussionPostsReadStatus
ON
 DiscussionPosts.topicid=DiscussionPostsReadStatus.TopicId AND  DiscussionPosts.PostId = DiscussionPostsReadStatus.PostId AND 
 DiscussionPosts.UserId = DiscussionPostsReadStatus.UserId
 
 SELECT Top 20 * FROM Discussions



 select Discussions.OrgUnitId,Discussions.UserId,Discussions.TopicId,
Discussions.PostId, Discussions.IsRead
into DiscussionReadperUser
from Discussions
INNER JOIN
Query
ON Query.UserId=Discussions.UserId AND Query.OrgUnitId = Discussions.OrgUnitId



select top 20  * from DiscussionReadperUser


---------Number of Unique posts read by a user---------

SELECT  OrgUnitId, UserId,COUNT(DISTINCT PostId) as PostIdCount,COUNT(DISTINCT TopicId) as TopicIdCount
FROM DiscussionReadperUser
GROUP BY OrgUnitId,UserId


#####################  Table (DiscussionReadperUser)########################################


OrgUnitId  | UserId | PostIdCount  | TopicIdCount


drop table DiscussionReadperUser





-----------------------Number of unique chatsessionid per user--------------

select ChatSessionLog.UserId,ChatSessionLog.OrgUnitId,ChatSessionLog.ChatSessionId
into ChatSessionperUser
from ChatSessionLog
INNER JOIN
Query
ON Query.UserId=ChatSessionLog.UserId AND Query.OrgUnitId = ChatSessionLog.OrgUnitId


select * from ChatSessionperUser

---------Number of unique ChatSessionId Per User---------

SELECT  OrgUnitId, UserId,COUNT(DISTINCT ChatSessionId) as ChatSessionIdCount
FROM ChatSessionperUser
GROUP BY OrgUnitId,UserId

#########################Table (ChatSessionperUser)#############################


OrgUnitId |  UserId |  ChatSessionIdCount


-- ######################## combined table with all the attributes    ############################

select 
t1.UserId,
t1.OrgUnitId,
t1.Organization,
t1.Type,
t1.Name,
t1.EnrollmentDate,
t2.DropboxIdCount,
t3.QuizIdCount,
t4.PostIdCount,
t4.TopicIdCount,
t5.ChatSessionIdCount
into tmp_data_combined
from Query t1 
left join assignmentCountPerUser1 t2 on t1.OrgUnitId = t2.OrgUnitId and t1.UserId = t2.UserId
left join QuizAttemptsPerUser1 t3 on t1.OrgUnitId = t3.OrgUnitId and t1.UserId = t3.UserId
left join DiscussionReadperUser1 t4 on t1.OrgUnitId = t4.OrgUnitId and t1.UserId = t4.UserId
left join ChatSessionperUser1 t5 on t1.OrgUnitId = t5.OrgUnitId and t1.UserId = t5.UserId

-- Remove all the courses where all of the features for all of the users is null 


select
distinct OrgUnitId 
into tmp_courses_to_be_removed
from tmp_data_combined 
where DropboxIdCount is null
and QuizIdCount is null 
and PostIdCount is null 
and TopicIdCount is null 
and ChatSessionIdCount is null 

select count(distinct OrgUnitId) from tmp_data_combined; --2613 
select count(*) from tmp_courses_to_be_removed; -- 2314 

select t1.*
into tmp_data_combined_2
from tmp_data_combined t1
where t1.OrgUnitId not in 
(select OrgUnitId from tmp_courses_to_be_removed)
-- 299 courses total 


select year(EnrollmentDate) as year ,month(EnrollmentDate) as month,count(*) from tmp_data_combined_2
group by year(EnrollmentDate),month(EnrollmentDate)
order by year(EnrollmentDate),month(EnrollmentDate)
-- most enrollments are in the year 2019 

select OrgUnitId,count(distinct UserId) as cnt 
from tmp_data_combined_2
group by OrgUnitId
order by count(distinct UserId) desc 
-- long tail distribution of users per course 
-- only 30 courses with a user count of 5 distinct users or more 

drop table if exists tmp_data_combined_3;

-- filter courses having users count >=5 
select 
t1.UserId,
t1.OrgUnitId,t1.Organization,t1.Type,t1.Name as CourseName,
t1.EnrollmentDate,
coalesce(t1.DropboxIdCount,0) as NumAssignmentSubmissions,
coalesce(t1.QuizIdCount,0) NumQuizDone,
coalesce(t1.PostIdCount,0) as NumPosts,
coalesce(t1.TopicIdCount,0) as NumTopics,
coalesce(t1.ChatSessionIdCount,0) as NumChatSessions
into tmp_data_combined_3
from tmp_data_combined_2 t1 
where OrgUnitId in 
(
select OrgUnitId
from tmp_data_combined_2
group by OrgUnitId
having count(distinct UserId) >= 5
); 







