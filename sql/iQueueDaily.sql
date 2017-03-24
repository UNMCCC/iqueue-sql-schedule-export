/********************************************************************************************************************************************
** iQueue DAILY FEED 
**  iQueue For Infusion Centers from LeanTasS iQueue:  Improve infusion scheduling through optimized templates.
**	
**    
**  Requirements:	
**		Daily Extract of Infusion Data that includes:
**			1) Scheduled and Actual treatments from yesterday's schedule.
**			2) Scheduled Infusion appointments for the next 2 months.
**	File Format:	CSV, comma separated
**	File Name:		unm_yyyy_mm_dd.csv 
**  Scheduling:		Query should automatically run after midnight and before start of business
**					Results should be sent via SFTP to SFTP server vault.leantass.com (port22) (using unm username and password)
**	 
******************************************************************************************************************************************/

/******************************************************************************************************************************************
** Retrieve actual infusion data from yesterday's schedule.  
**		This includes patients who received infusion, 
**		patients who were present by not treated (e.g. physician canceled chemo because of blood work results), 
**		patients who canceled or were no-shows.
**	Need schedule and queue data
*******************************************************************************************************************************************/

/*******************************************************************************************************************************************
** Get yesterday's schedule for MedOnc and eliminate sample, inactive, and deceased patients
** Gather all pertinent scheduling data
** Do not restrict query to infusion activities/locations because sometimes a patient who was 
** scheduled for a non-infusion activity (e.g. Injection) or at a non-infusion location (e.g. Shot Clinic actually receives infusion.
** These are identified because patient received treatment in(...were queued to) a bed or chair.  
** Scheduling info will be needed for those situations.** By gathering all scheduling data here, it will be available for this case.
********************************************************************************************************************************************/ 

/******Get All Schedules from Yesterday through 2 months in the Future **********************************************************************/


select distinct 
	A.S_Pat_Name, 
	A.S_MRN, 
	A.S_pat_id1,  
	A.S_sch_id, 
	A.S_app_dtTm, 
	A.S_app_dt,
	A.S_Duration_Min,
	A.S_location, 
	A.S_activity,
	A.S_PrimStatCd,
	A.S_SecStatCd,
	A.S_PrimaryStatus,
	A.S_SecondaryStatus,
	A.S_edit_DtTm,
	A.S_create_dtTm
into #Sched
from (
			select 
			vS.PAT_NAME as S_Pat_Name,
			ident.IDA as S_mrn,
			vs.pat_id1 as S_Pat_id1,
			vs.sch_id as S_sch_id, 
			vs.App_dtTm S_App_DtTm,
			convert(char(8),vS.App_DtTm,112) as S_app_dt, 
			cast(rtrim(replace(isnull(dbo.fn_ConvertTimeIntToDurationinMin(vS.Duration_time), 0),'mins', ' ')) as integer) as S_Duration_Min,
			isnull(vs.Location, ' ') as S_Location, -- location where patient is scheduled
			isnull(vS.short_Desc, ' ') as S_Activity, -- short description of activity patient is scheduled for
			isnull(vS.SysDefStatus, ' ') as S_PrimStatCd, 
			isnull(vS.UserDefStatus, ' ') as S_SecStatCd, 

			case	when vS.SysDefStatus = 'N' then 'No Show'
					when vS.sysDefStatus = 'X' then 'Canceled' 
					when vS.SysDefStatus = 'E' then 'Ended'
					when vS.SysDefStatus = ' C'  or SysDefStatus = 'OC' or SysDefStatus = 'SC' or SysDefStatus = 'FC' then 'Completed'
					when vS.SysDefStatus = 'B'  then 'Break'
					when vS.SysDefStatus = 'M' then 'Machine Down'
					when vS.SysDefStatus = 'F' then 'Final'
					when (vS.SysDefStatus = ' ' or vS.SysDefStatus is null) then 'Unresolved'
					else 'Other'
			end S_PrimaryStatus,
			
			case	when vS.UserDefStatus = 'HS' then 'Hospitalization' 
					when vS.UserDefStatus = 'MR' then 'MD Request'
					when vS.UserDefStatus = 'PR' then 'Patient Request'
					when vS.UserDefStatus = 'SE' then 'Scheduled in Error'
	     			when vS.UserDefStatus = 'NT' then 'No Treatment'
					when vS.UserDefStatus = 'RS' then 'Rescheduled'
					when vS.UserDefStatus = 'WO' then 'Walk Out'
					when vS.UserDefStatus = 'MD' then 'MD Request'
					when vS.UserDefStatus = 'HS' then 'Hospitalization'
					when vS.UserDefStatus is null then ' '
					else 'other' -- 'CF', 'DP', 'IN', 'P2', 'P4', 'P5', 'RC'
			end S_SecondaryStatus,
			
			
			sch.Edit_DtTm as S_edit_DtTm,
 			sch.create_dtTm  as S_create_dtTm,	
 						
 			case when Ident.IDA = ' ' or Ident.IDA = '***************' or Ident.IDA = '00000000000'  or Ident.IDA is null or Ident.IDA like '%Do Not Use%' or Ident.IDA = '123' or Patient.Last_Name = 'TEST'
				then 'YES'
				else 'NO'
			end IsSamplePatient,
			
			case when (admin.Expired_DtTm is not null and admin.Expired_DtTm <> ' ') or (Ident.IDC is not null and Ident.IDC <> ' ')
				then 'YES' 
				else 'NO'
			end IsDeceased,
				
			case when Patient.Inactive <> 0 
				then 'YES'
				else 'NO'
			end IsInactive	
	from vw_Schedule vS
		left join Ident		on vS.Pat_ID1 = Ident.Pat_id1
		left join Admin		on vS.Pat_ID1 = Admin.Pat_id1
		left join Patient	on vS.Pat_ID1 = Patient.Pat_ID1
		left join Schedule sch on vS.Pat_ID1 = sch.Pat_ID1 and vS.Sch_Id = sch.sch_id 
	where	 (datename(dw,GetDate()) = 'Monday' and convert(char(8),vS.App_DtTm,112) >= CONVERT(char(8), GETDATE() -3, 112) or  -- yesterday is Friday if today is Monday
			  datename(dw,GetDate()) <> 'Monday'  and convert(char(8),vS.App_DtTm,112) >= CONVERT(char(8), GETDATE() -1, 112)    -- yesterday is yesterday if today is not Monday
			 )
														
		and convert(char(8),vS.App_DtTm,112) <= CONVERT(char(8), GETDATE()+ 60, 112) -- 2 months from today
		and vS.DEPT = 'UNMMO'			-- MO (not RO) appointments
) as A	
where isInactive = 'NO' and isSamplePatient = 'NO' and isDeceased = 'NO'


/* Get Patients who were Queued for infusion yesterday */
select distinct  
		vQ.App_DtTm as Q_App_DtTm,
		vQ.Pat_id1 as Q_Pat_ID1,
 		isnull(vQ.QueLoc, ' ') as Q_Location,
 		isnull(dbo.fn_ConvertTimeIntToDurationhrmin(vQ.Arrived), ' ') as Q_Arrived,
		isnull(dbo.fn_ConvertTimeIntToDurationhrmin(vQ.TransItem), ' ') as Q_Start_Infusion,
		isnull(dbo.fn_ConvertTimeIntToDurationhrmin(vQ.Complete_time), ' ') as Q_End_Infusion,
		isnull(vQ.Complete, ' ') as Q_Complete,
		isnull(vQ.Sch_Id , ' ')as Q_Sch_id
into #Queued
from vw_QueBro as vQ
where (vQ.QueLoc like '%Chair%' or vQ.QueLoc like '%Bed%'  or vQ.QueLoc like '%Infusion%') -- get actual location from Alicia - this will be a new designation
		and (datename(dw,GetDate()) = 'Monday' and convert(char(8),vQ.App_DtTm,112) >= CONVERT(char(8), GETDATE() -3, 112) or  -- yesterday is Friday if today is Monday
			 datename(dw,GetDate()) <> 'Monday'  and convert(char(8),vQ.App_DtTm,112) >= CONVERT(char(8), GETDATE() -1, 112)    -- yesterday is yesterday if today is not Monday
			 )
	  and vQ.Version = 0
	  and vQ.Pat_ID1 is not null


/* Combine Actual(#Queued) with Scheduled (#Sched) data to get a complete list of patients who were scheduled and treated(queued), not treated, canceled or were no-shows */
select distinct
	#Sched.S_app_Dt,  --remove
	#Sched.S_app_dtTm,
	#Sched.S_Pat_Name, --remove
	#Sched.S_MRN, --remove
	#Sched.S_Pat_id1,
	#Sched.S_sch_id, --remove
	#Sched.S_Duration_Min,
	#Sched.S_location, 
	#Sched.S_Activity,  
	#Sched.S_PrimStatCd, 
	#Sched.S_SecStatCd,
	#Sched.S_PrimaryStatus,
	#Sched.S_SecondaryStatus,
	#Sched.S_edit_DtTm,
	#Sched.S_create_dtTm,
	#Queued.Q_Location,
	#Queued.Q_Arrived,
	#Queued.Q_Start_Infusion,
	#Queued.Q_End_Infusion
into #Yesterday
from #Sched 
join #Queued on #Sched.S_Pat_ID1 = #Queued.Q_pat_id1 and  #Sched.S_Sch_id = #Queued.Q_Sch_id 
where    (datename(dw,GetDate())  = 'Monday' and #Sched.S_app_Dt = CONVERT(char(8), GETDATE() -3, 112))
	  or (datename(dw,GetDate()) <> 'Monday' and #Sched.S_app_Dt = CONVERT(char(8), GETDATE() -1, 112))
			





/****************** FUTURE *******************************************************************************************************/
select distinct 
	#Sched.S_app_dt, --remove
	#Sched.S_app_dtTm, 
	#Sched.S_Pat_Name,  --remove
	#Sched.S_MRN,  --remove
	#Sched.S_pat_id1,  
	#Sched.S_sch_id,  --remove
	#Sched.S_Duration_Min,
	#Sched.S_location, 
	#Sched.S_activity,
	#Sched.S_PrimStatCd,
	#Sched.S_SecStatCd,
	#Sched.S_PrimaryStatus,
	#Sched.S_SecondaryStatus,
	#Sched.S_edit_DtTm,
	#Sched.S_create_dtTm,
	' ' as Q_Location,
	' ' as Q_Arrived,
	' ' as Q_Start_Infusion,
	' ' as Q_End_Infusion
into #Future
from #Sched
	where	convert(char(8),#Sched.S_App_DtTm,112) >= CONVERT(char(8), GETDATE() , 112)    -- today
	and 
  		(  #Sched.S_Location like '%Infusion%'		-- need to get actual location from Alicia - this will be a new designation
		or #Sched.S_Location like '%Chair%'		-- Bed and Chair are current designations for scheduled locations 
                                                                    -- this will change with go-live of new scheduling template
		or #Sched.S_Location like '%Bed%'		 -- any appt scheduled in a bed or chair is considered an infusion-suite appointment
		or #Sched.S_Activity like '%Infusion%'		     -- e.g. 1 Hr Infusion Apt, Bed Infusion 3 Hours, sq infusion...
		or #Sched.S_Activity like '%Blood Trans%'    	     -- e.g. Blood Trans Apt, Blood Transfusion
		or #Sched.S_Activity like '%Platelet Trans%'	     -- e.g. Platelet Trans Appt, Plantelet Transfusion
		or #Sched.S_Activity like '%Transfusion%'            -- e.g. Blood Transfusion, Transfusion 2 Hours, Transfustion 3 Hours...
		or #Sched.S_Activity like '%Platelets%' 
		or #Sched.S_Activity like '%Phlebotomy%'	
		or #Sched.S_Activity = 'IV Chemo Initial Hr'		-- e.g. Chemo New Start, Chemo Teach, IV Chemo Initial Hr, SQ/IM Hormonal Chemo...
		or #Sched.S_Activity = 'IV P Chemo Initial'
		or #Sched.S_Activity = 'NC IV Push Initial'		-- don't search for '%Chemo%' because will get pre chemo office visits
		or #Sched.S_Activity = 'SQ/IM Hormonal Chemo'
		or #Sched.S_Activity = 'SQ/IM NH Chemo'
		or #Sched.S_Activity =  'Hydration'
		or #Sched.S_Activity like '%Chemo Teach%'
		or #Sched.S_Activity like '%Chemo New Start%'
		or #Sched.S_Activity like '%Stem Cell%'
		or #Sched.S_Activity like '%Bladder instil. chem%'
		or #Sched.S_Activity like '%Observation%'
	)


/* Union together data from #Yesterday with #Future.  Send these results to iQueue */
select 
	'Past'		as Rec_Type,
	'Infusion'	as Visit_Type,
	S_Pat_id1	as Internal_Patient_ID,
	S_Activity	as Appt_Type,
	S_App_dtTm	as Appt_DtTm,
	S_Duration_min	as Expected_Duration,
	S_Location		as Scheduled_Location,
	S_PrimStatCd	as Primary_Status_Code,
	S_PrimaryStatus as Primary_Status,
	S_SecStatCd		as Secondary_Status_Code,
	S_SecondaryStatus	as Secondary_Status,
	S_Create_DtTm		as Appt_Made_DtTm,
	S_Edit_DtTm			as Appt_Changed_DtTm,
	Q_Location			as Actual_Location,
	Q_Arrived			as CheckIn_Tm,
	Q_Start_Infusion	as Infusion_Start_Tm,
	Q_End_infusion		as Infusion_Stop_Tm
from #Yesterday	
union
select 
	'Future'	as Rec_Type,
	'Infusion'	as Visit_Type,
	S_Pat_id1	as Internal_Patient_ID,
	S_Activity	as Appt_Type,
	S_App_dtTm	as Appt_DtTm,
	S_Duration_min	as Expected_Duration,
	S_Location		as Scheduled_Location,
	S_PrimStatCd	as Primary_Status_Code,
	S_PrimaryStatus as Primary_Status,
	S_SecStatCd		as Secondary_Status_Code,
	S_SecondaryStatus	as Secondary_Status,
	S_Create_DtTm		as Appt_Made_DtTm,
	S_Edit_DtTm			as Appt_Changed_DtTm,
	Q_Location			as Actual_Location,
	Q_Arrived			as CheckIn_Tm,
	Q_Start_Infusion	as Infusion_Start_Tm,
	Q_End_infusion		as Infusion_Stop_Tm
from #Future