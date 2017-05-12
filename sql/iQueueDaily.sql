/********************************************************************************************************************************************************************************
**	Name:		iQueueDaily.sql
**	Purpose:	Extract UNMCCC Infusion Data from Elekta MOSAIQ Oncology System to be sent to LeanTasS and analyzed via "iQueue For Infusion Centers", 
**				which creates optimized templates to improve infusion scheduling
**	Go-Live:	May 2017
**	
**	Requirements:	
**		Daily Extract of Infusion Data that includes:
**			1) Scheduled and Actual infusion appointments from yesterday's schedule.
**			2) Status of yesterday's appointments (eg. completed, cancelled, no-show...)
**			2) Scheduled Infusion appointments for the next 2 months.
**
**	File Format:	CSV, comma separated
**	File Name:		unmcc_yyyy_mm_dd.csv 
**  Scheduling:		Query runs automatically Monday-Friday at 4:30 AM MST
**					Results are uploaded to SFTP server vault.leantass.com (port22) (using unm username and password)
*******************************************************************************************************************************************************************************/


/* Set NoCount on to suppress record counts from exported file that is sent to iQueue*/
SET NOCOUNT ON;

	
			

/******************************************************************************************************************************************************************************
**  Gather Schedule and Queue data for scheduled and actual infusion visits for "yesterday" ("Yesterday"=Friday if "today" is Monday).  
**	Scenarios:
**		1) Scheduled for Infusion and Received Infusion
**		2) Scheduled for Infusion but didn't receive it (physician cancelled chemo because of blood work results, patient cancelled/rescheduled, 
**			patient in hospital, patient is a no-show)
**		3) Patient was treated at the infusion suite (i.e. in a bed or chair), but was not scheduled for infusion (e.g. was incorrectly scheduled
**			for an injection at the Shot Clinic for 15 minutes, but received 2 hour infusion)	
**
**	Tech notes:
**		1)	Get "yesterday's" data from dbo.vw_QueBro (Queue Browser view)which combines data from the Queue (dbo.Que) table with Config, Patient, Staff, and Schedule tables
**		2)	Get Scheduling data from associated vw_Schedule and Schedule (for create and edit dates)
**	Data Notes
**		Infusion visits are identified by the location where the patient was queued
**			Examples of Infusion queuing locations are 'S Bed 10', 'N Chair 5', '4th Floor Infusion', 'Infusion Add On'
**			This necessiates the use of LIKE in the WHERE clause, even though this is not best practice.
**		Use Queue Browser View (dbo.vw_QueBro) which combines data from the Queue (dbo.Que) table with Config, Patient, Staff, and Schedule tables
******************************************************************************************************************************************************************************/
/* Get Infusions that were Queued "yesterday" */
SELECT  
		dbo.fn_GetPatientName(vwQB.pat_id1,'NAMELFM') as PatName,
		vwQB.Pat_id1, 
		vwQB.App_DtTm,
 		vwQB.QueLoc,
 		vwQB.Arrived,
		vwQB.TransItem,
		vwQB.Complete_time,
		vwQB.Sch_Id
into #Queued	
FROM MOSAIQ.dbo.vw_QueBro AS vwQB
WHERE vwQB.Version = 0   
	AND (
		DATENAME(weekday,GETDATE()) =  'Monday' and CONVERT(char(8),vwQB.App_DtTm,112) = CONVERT(char(8), GETDATE() -3, 112)      
	 OR DATENAME(weekday,GETDATE()) <> 'Monday' and CONVERT(char(8),vwQB.App_DtTm,112) = CONVERT(char(8), GETDATE() -1, 112) 
		)
AND
	(vwQB.QueLoc like '%Chair%' or vwQB.QueLoc like '%Bed%'  or vwQB.QueLoc like '%Infusion%')
;  /* End */



/**Get Patients who were Schedule for Infusion Appointments "yesterday" ****/
SELECT
	pat_id1,
	App_DtTm		AS S_ApptDtTm,
	Sch_id			AS S_SchID,
	Duration_Time	AS S_Duration,
	Location		AS S_Location,
	Short_Desc		AS S_Activity,
	SysDefStatus	AS S_PrimStatCd,
	UserDefStatus	AS S_SecStatCd,
	Edit_DtTm		AS S_EditDtTm,
	Create_DtTm		AS S_CreateDtTm,
	QueLoc			AS Q_Location,
	Arrived			AS Q_Arrived,
	TransItem		AS Q_TransItem,
	Complete_time	AS Q_Complete
INTO #Yesterday
FROM 
	(
	SELECT 
		vwSCH.PAT_NAME as PatName,
		vwSCH.pat_id1,
		vwSCH.App_dtTm,
		vwSCH.sch_id, 
		vwSCH.Duration_time,
		vwSCH.Location,	 
		vwSCH.short_Desc,	  
		vwSCH.SysDefStatus,
		vwSCH.UserDefStatus,
		SCH.Edit_DtTm,
		SCH.Create_dtTm,
		#Queued.Sch_Id as Q_Sch_id,
		#Queued.QueLoc,
 		#Queued.Arrived,
		#Queued.TransItem,
		#Queued.Complete_time,
		CASE
			WHEN I.IDA = ' ' OR I.IDA = '***************' OR I.IDA = '00000000000'  OR I.IDA IS NULL OR I.IDA LIKE '%Do Not Use%' OR I.IDA = '123'
				 OR P.Last_Name = 'TEST' OR vwSCH.Pat_Name = 'SAMPLE, PATIENT'   
			THEN 'YES'
			ELSE 'NO'
		END IsSamplePatient,
		CASE
			WHEN (A.Expired_DtTm IS NOT NULL AND A.Expired_DtTm <> ' ') 
			  OR (I.IDC IS NOT NULL AND I.IDC <> ' ')
			THEN 'YES' 
			ELSE 'NO'
		END IsDeceased,
		CASE
			WHEN P.Inactive <> 0 
			THEN 'YES'
			ELSE 'NO'
		END IsInactive	
	FROM MOSAIQ.dbo.vw_Schedule vwSCH
	JOIN MOSAIQ.dbo.Schedule AS SCH ON vwSCH.Pat_ID1 = SCH.Pat_ID1 and vwSCH.Sch_ID = SCH.Sch_iD 
	LEFT OUTER JOIN #Queued on vwSCH.Sch_Id = #Queued.Sch_id
	LEFT JOIN MOSAIQ.dbo.Staff   AS Staff on vwSCH.Staff_ID = Staff.Staff_ID
	JOIN MOSAIQ.dbo.Ident	AS I ON vwSCH.Pat_ID1 = I.Pat_ID1
	JOIN MOSAIQ.dbo.Admin	AS A ON vwSCH.Pat_ID1 = A.Pat_ID1
	JOIN MOSAIQ.dbo.Patient	AS P ON vwSCH.Pat_ID1 = P.Pat_ID1
	WHERE vwSCH.Dept = 'UNMMO'
		 AND ( -- Yesterday's Scheduled Appointments/Queued Visits
				DATENAME(weekday,GETDATE()) =  'Monday' and CONVERT(char(8),vwSCH.App_DtTm,112)= CONVERT(char(8), GETDATE()-3,112)    
		  OR	DATENAME(weekday,GETDATE()) <> 'Monday' and CONVERT(char(8),vwSCH.App_DtTm,112)= CONVERT(char(8), GETDATE()-1,112)     
			 )
		AND 
			(
				vwSCH.Location LIKE '%Infusion%'		-- '4th floor infusion', 'Infusion Add On'
				OR vwSCH.Location LIKE '%Chair%'		-- Bed and Chair are current designations for scheduled locations 
  				OR vwSCH.Location LIKE '%Bed%'			-- any appt scheduled in a bed or chair is considered an infusion-suite appointment
				OR vwSCH.Short_Desc LIKE '%Infusion%'		     -- e.g. 1 Hr Infusion Apt, Bed Infusion 3 Hours, sq infusion...
				OR vwSCH.Short_Desc LIKE '%Blood Trans%'    	     -- e.g. Blood Trans Apt, Blood Transfusion
				OR vwSCH.Short_Desc LIKE '%Platelet Trans%'	     -- e.g. Platelet Trans Appt, Plantelet Transfusion
				OR vwSCH.Short_Desc LIKE '%Transfusion%'            -- e.g. Blood Transfusion, Transfusion 2 Hours, Transfusion 3 Hours...
				OR vwSCH.Short_Desc LIKE '%Platelets%' 
				OR vwSCH.Short_Desc LIKE '%Phlebotomy%'	
				OR vwSCH.Short_Desc = 'IV Chemo Initial Hr'		-- e.g. Chemo New Start, Chemo Teach, IV Chemo Initial Hr, SQ/IM Hormonal Chemo...
				OR vwSCH.Short_Desc = 'IV P Chemo Initial'
				OR vwSCH.Short_Desc = 'NC IV Push Initial'		-- don't search for '%Chemo%' because will get pre chemo office visits
				OR vwSCH.Short_Desc = 'SQ/IM Hormonal Chemo'
				OR vwSCH.Short_Desc = 'SQ/IM NH Chemo'
				OR vwSCH.Short_Desc =  'Hydration'
				OR vwSCH.Short_Desc LIKE '%Chemo Teach%'
				OR vwSCH.Short_Desc LIKE '%Chemo New Start%'
				OR vwSCH.Short_Desc LIKE '%Stem Cell%'
				OR vwSCH.Short_Desc LIKE '%Bladder instil. chem%'
				OR vwSCH.Short_Desc LIKE '%Observation%'
				OR #Queued.Sch_id IS NOT NULL  --- Actual infusion visit but scheduled for non-infusion appointment
				OR Staff.Last_Name = 'Infusion' -- Staff Name is used for 
			)
	) AS A
WHERE IsSamplePatient = 'NO' and IsDeceased = 'NO' and IsInactive = 'NO'

/****************** FUTURE *******************************************************************************************************/
SELECT  
	Pat_id1,
	App_DtTm		AS S_ApptDtTm,
	Sch_id			AS S_SchID,
	Duration_Time	AS S_Duration,
	Location		AS S_Location,
	Short_Desc		AS S_Activity,
	SysDefStatus	AS S_PrimStatCd,
	UserDefStatus	AS S_SecStatCd,
	Edit_DtTm		AS S_EditDtTm,
	Create_DtTm		AS S_CreateDtTm
INTO #Future
FROM
	(
	SELECT 
		vwSCH.PAT_NAME as PatName,
		vwSCH.pat_id1,
		vwSCH.App_dtTm,
		vwSCH.sch_id, 
		vwSCH.Duration_time,
		vwSCH.Location,	 
		vwSCH.short_Desc,	  
		vwSCH.SysDefStatus,
		vwSCH.UserDefStatus,
		SCH.Edit_DtTm,
		SCH.Create_dtTm,
		CASE
			WHEN I.IDA = ' ' OR I.IDA = '***************' OR I.IDA = '00000000000'  OR I.IDA IS NULL OR I.IDA LIKE '%Do Not Use%' OR I.IDA = '123'
				 OR P.Last_Name = 'TEST' OR vwSCH.Pat_Name = 'SAMPLE, PATIENT'   
			THEN 'YES'
			ELSE 'NO'
		END IsSamplePatient,
		CASE
			WHEN (A.Expired_DtTm IS NOT NULL AND A.Expired_DtTm <> ' ') 
			  OR (I.IDC IS NOT NULL AND I.IDC <> ' ')
			THEN 'YES' 
			ELSE 'NO'
		END IsDeceased,
		CASE
			WHEN P.Inactive <> 0 
			THEN 'YES'
			ELSE 'NO'
		END IsInactive	
	FROM MOSAIQ.dbo.vw_Schedule vwSCH
	JOIN MOSAIQ.dbo.Schedule AS SCH ON vwSCH.Pat_ID1 = SCH.Pat_ID1 and vwSCH.Sch_ID = SCH.Sch_iD 
	LEFT JOIN MOSAIQ.dbo.Staff   AS Staff on vwSCH.Staff_ID = Staff.Staff_ID
	JOIN MOSAIQ.dbo.Ident	AS I ON vwSCH.Pat_ID1 = I.Pat_ID1
	JOIN MOSAIQ.dbo.Admin	AS A ON vwSCH.Pat_ID1 = A.Pat_ID1
	JOIN MOSAIQ.dbo.Patient	AS P ON vwSCH.Pat_ID1 = P.Pat_ID1
	WHERE	vwSCH.Dept = 'UNMMO'
		AND convert(char(8),vwSCH.App_DtTm,112) >= CONVERT(char(8),GETDATE(),112)    -- today
		AND CONVERT(char(8),vwSCH.App_DtTm,112) <= CONVERT(char(8),GETDATE()+60,112) -- 2 months from today
		
		AND 
			(
				vwSCH.Location LIKE '%Infusion%'		-- '4th floor infusion', 'Infusion Add On'
				OR vwSCH.Location LIKE '%Chair%'		-- Bed and Chair are current designations for scheduled locations 
  				OR vwSCH.Location LIKE '%Bed%'			-- any appt scheduled in a bed or chair is considered an infusion-suite appointment
				OR vwSCH.Short_Desc LIKE '%Infusion%'		     -- e.g. 1 Hr Infusion Apt, Bed Infusion 3 Hours, sq infusion...
				OR vwSCH.Short_Desc LIKE '%Blood Trans%'    	     -- e.g. Blood Trans Apt, Blood Transfusion
				OR vwSCH.Short_Desc LIKE '%Platelet Trans%'	     -- e.g. Platelet Trans Appt, Plantelet Transfusion
				OR vwSCH.Short_Desc LIKE '%Transfusion%'            -- e.g. Blood Transfusion, Transfusion 2 Hours, Transfusion 3 Hours...
				OR vwSCH.Short_Desc LIKE '%Platelets%' 
				OR vwSCH.Short_Desc LIKE '%Phlebotomy%'	
				OR vwSCH.Short_Desc = 'IV Chemo Initial Hr'		-- e.g. Chemo New Start, Chemo Teach, IV Chemo Initial Hr, SQ/IM Hormonal Chemo...
				OR vwSCH.Short_Desc = 'IV P Chemo Initial'
				OR vwSCH.Short_Desc = 'NC IV Push Initial'		-- don't search for '%Chemo%' because will get pre chemo office visits
				OR vwSCH.Short_Desc = 'SQ/IM Hormonal Chemo'
				OR vwSCH.Short_Desc = 'SQ/IM NH Chemo'
				OR vwSCH.Short_Desc =  'Hydration'
				OR vwSCH.Short_Desc LIKE '%Chemo Teach%'
				OR vwSCH.Short_Desc LIKE '%Chemo New Start%'
				OR vwSCH.Short_Desc LIKE '%Stem Cell%'
				OR vwSCH.Short_Desc LIKE '%Bladder instil. chem%'
				OR vwSCH.Short_Desc LIKE '%Observation%'
				OR Staff.Last_Name = 'Infusion'
			)
	) AS A
WHERE IsSamplePatient = 'NO' and IsDeceased = 'NO' and IsInactive = 'NO'

/*********************************************************************************************/
/* Union together data from #Yesterday with #Future.  Send these results to iQueue */
SELECT
	Rec_Type,
	Unit,
	Visit_Type,
	Pat_id1						AS Internal_Patient_ID,
	ISNULL(S_Activity, ' ')		AS Appt_Type,
	S_ApptdtTm					AS Appt_DtTm,
	CAST(RTRIM(REPLACE(ISNULL(dbo.fn_ConvertTimeIntToDurationinMin(S_Duration), 0),'mins', ' ')) AS INTEGER) as S_DurationMin,
	ISNULL(S_Location, ' ')		AS Scheduled_Location,
	ISNULL(S_PrimStatCd, ' ')	AS Primary_Status_Code,
	CASE 
		WHEN S_PrimStatCd = 'N'	THEN 'No Show'
		WHEN S_PrimStatCd = 'X'	THEN 'Canceled' 
		WHEN S_PrimStatCd = 'E'	THEN 'Ended'
		WHEN S_PrimStatCd = ' C' OR S_PrimStatCd = 'OC' OR S_PrimStatCd = 'SC' OR S_PrimStatCd = 'FC' THEN 'Completed'
		WHEN S_PrimStatCd = 'B'  THEN 'Break'
		WHEN S_PrimStatCd = 'M'  THEN 'Machine Down'
		WHEN S_PrimStatCd = 'F'  THEN 'Final'
		WHEN (S_PrimStatCd = ' ' OR S_PrimStatCd IS NULL) then 'Unresolved'
		ELSE 'Other'
	END Primary_Status,
	
	ISNULL(S_SecStatCd, ' ')		AS Secondary_Status_Code,
	
	CASE
		WHEN S_SecStatCd = 'HS' THEN 'Hospitalization' 
		WHEN S_SecStatCd = 'MR' THEN 'MD Request'
		WHEN S_SecStatCd = 'PR' THEN 'Patient Request'
		WHEN S_SecStatCd = 'SE' THEN 'Scheduled in Error'
 		WHEN S_SecStatCd = 'NT' THEN 'No Treatment'
		WHEN S_SecStatCd = 'RS' THEN 'Rescheduled'
		WHEN S_SecStatCd = 'WO' THEN 'Walk Out'
		WHEN S_SecStatCd = 'MD' THEN 'MD Request'
		WHEN S_SecStatCd = 'HS' THEN 'Hospitalization'
		WHEN S_SecStatCd IS NULL THEN ' '
		ELSE 'Other' 
	END S_SecondaryStatus,
	
	S_CreateDtTm				AS Appt_Made_DtTm,
	S_EditDtTm					AS Appt_Changed_DtTm,
	ISNULL(Q_Location, ' ')		AS Actual_Location,
	ISNULL(dbo.fn_ConvertTimeIntToDurationhrmin(Q_Arrived),' ')		 	AS CheckIn_Tm,
	ISNULL(dbo.fn_ConvertTimeIntToDurationhrmin(Q_TransItem),' ')		AS Infusion_Start_Tm,
	ISNULL(dbo.fn_ConvertTimeIntToDurationhrmin(Q_Complete), ' ')		AS Infusion_Stop_Tm
FROM 
	(
	SELECT 
		'Past'		AS Rec_Type,
		'4th Floor Infusion' AS Unit,
		'Infusion'	AS Visit_Type,
		Pat_id1,
		S_Activity,
		S_ApptDtTm,
		S_Duration,
		S_Location,
		S_PrimStatCd,
		S_SecStatCd,
		S_CreateDtTm,
		S_EditDtTm,
		Q_Location,
		Q_Arrived,
		Q_TransItem,
		Q_Complete
	FROM #Yesterday
	UNION
	SELECT 
		'Future'	AS Rec_Type,
		'4th Floor Infusion' AS Unit,
		'Infusion'	AS Visit_Type,
		Pat_id1,
		S_Activity,
		S_ApptDtTm,
		S_Duration,
		S_Location,
		S_PrimStatCd,
		S_SecStatCd,
		S_CreateDtTm,
		S_EditDtTm,
		' ' AS Q_Location,
		' ' AS Q_Arrived,
		' ' AS Q_TransItem,
		' ' AS Q_Complete
	FROM #Future
	) as A

