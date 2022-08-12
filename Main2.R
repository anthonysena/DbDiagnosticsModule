# Copyright 2022 Observational Health Data Sciences and Informatics
#
# This file is part of dbProfileModule
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Module methods -------------------------
execute <- function(jobContext) {
	rlang::inform("Validating inputs")
	checkmate::assert_list(x = jobContext)
	
# Discuss with Sena  
# Is jobContext$settings how we would set environmental 
# variables for bulk loading?
#
#	if (is.null(jobContext$settings)) {
#		stop("Analysis settings not found in job context")
#	}

# Discuss with Sena  
#  if (is.null(jobContext$sharedResources)) {
#    stop("Shared resources not found in job context")
#  }

	if (is.null(jobContext$moduleExecutionSettings)) {
		stop("Execution settings not found in job context")
	}

	if (jobContext$moduleExecutionSettings$runDQD) {
		msg <- "Executing Achilles (if results are not found) and DQD."
	} else {
		msg <- "Executing Achilles (if results are not found) only."
	}	

	rlang::inform(msg)

	dbId <- getStringDatabaseId(
		jobContext$moduleExecutionSettings$connectionDetails,
		jobContext$moduleExecutionSettings$cdmDatabaseSchema)

	databaseId <- getHashedDatabaseId(
		jobContext$moduleExecutionSettings$connectionDetails,
		jobContext$moduleExecutionSettings$cdmDatabaseSchema)
		
	DbProfile::execute(
	  connectionDetails      = jobContext$moduleExecutionSettings$connectionDetails,
	  cdmDatabaseSchema      = jobContext$moduleExecutionSettings$cdmDatabaseSchema,
	  resultsDatabaseSchema  = jobContext$moduleExecutionSettings$resultsDatabaseSchema,
	  vocabDatabaseSchema    = jobContext$moduleExecutionSettings$vocabDatabaseSchema,
	  cdmSourceName          = dbId,
	  outputFolder           = jobContext$moduleExecutionSettings$outputFolder,
	  cdmVersion             = jobContext$moduleExecutionSettings$cdmVersion,
	  overwriteAchilles      = jobContext$moduleExecutionSettings$overwriteAchilles,
	  minCellCount           = jobContext$moduleExecutionSettings$minCellCount,
	  tableCheckThresholds   = jobContext$moduleExecutionSettings$tableCheckThresholds,
	  fieldCheckThresholds   = jobContext$moduleExecutionSettings$fieldCheckThresholds,
	  conceptCheckThresholds = jobContext$moduleExecutionSettings$conceptCheckThresholds,
	  addDQD                 = jobContext$moduleExecutionSettings$runDQD)
	
	rlang::inform("Zipping up results")
	zipFile <- combineResults(
		jobContext$moduleExecutionSettings$cdmDatabaseSchema,
		jobContext$moduleExecutionSettings$outputFolder)

	rlang::inform(paste("Final results are now available in: ", zipFile))	

	rlang::inform("Upload Achilles results")
	uploadAchillesResults(
		jobContext$moduleExecutionSettings$uploadConnectionDetails,
		jobContext$moduleExecutionSettings$uploadResultsSchema,
		jobContext$moduleExecutionSettings$cdmDatabaseSchema,
		jobContext$moduleExecutionSettings$outputFolder,
		dbId,
		databaseId)
  
	if (jobContext$moduleExecutionSettings$runDQD) {
		rlang::inform("Upload DQD results")
		uploadDQDResults(
			jobContext$moduleExecutionSettings$uploadConnectionDetails,
			jobContext$moduleExecutionSettings$uploadResultsSchema,
			jobContext$moduleExecutionSettings$cdmDatabaseSchema,
			jobContext$moduleExecutionSettings$outputFolder,
			dbId,
			databaseId)
	}
}

# Private methods -------------------------

getStringDatabaseId <- function(connectionDetails,cdmDatabaseSchema) {
	dbId <- AresIndexer::getSourceReleaseKey(connectionDetails,cdmDatabaseSchema)
	dbId <- gsub("/","-",dbId)
	return (dbId)
}

getHashedDatabaseId <- function(connectionDetails,cdmDatabaseSchema) {
	sql <- sprintf("select cdm_source_name,cdm_release_date from %s.cdm_source;",cdmDatabaseSchema)
	conn <- DatabaseConnector::connect(connectionDetails)
	cdmSourceData <- DatabaseConnector::querySql(conn,sql)
	DatabaseConnector::disconnect(conn)
	databaseId <- digest::digest2int(paste(cdmSourceData$CDM_SOURCE_NAME,cdmSourceData$CDM_RELEASE_DATE))
	return (databaseId)
}

# Zip Achilles and DQD results
combineResults <- function (cdmDatabaseSchema,outputFolder) {

	cdmReleaseName   <- substr(cdmDatabaseSchema,5,nchar(cdmDatabaseSchema))
	cdmReleaseFolder <- paste(outputFolder,cdmReleaseName,sep = "/")
	
	outputFile <- file.path(cdmReleaseFolder, paste0("DbProfileResults_", cdmReleaseName, ".zip"))
	
	zip(zipfile = outputFile,
		c(paste(cdmReleaseFolder,"achilles_results.csv",sep = "/"),
		  paste(cdmReleaseFolder,"achilles_results_augmented.csv", sep = "/"),
		  paste(cdmReleaseFolder,paste(cdmSourceName,"DbProfile.json",sep = "_"), sep="/")),
		  extras = '-j')
	
	return (outputFile)
}

# Upload Achilles results to upload results schema
uploadAchillesResults <- function(
	uploadConnectionDetails,
	uploadResultsSchema,
	cdmDatabaseSchema,
	outputFolder,
	dbId,
	databaseId) 
{

	cdmReleaseName   <- substr(cdmDatabaseSchema,5,nchar(cdmDatabaseSchema))
	cdmReleaseFolder <- paste(outputFolder,cdmReleaseName,sep = "/")
	
	achillesResults.csv <- read.csv(paste0(cdmReleaseFolder,"/achilles_results.csv"), stringsAsFactors = F)
	achillesResults.csv$DATABASE_ID <- databaseId
	achillesResults.csv$DB_ID <- dbId
	dp_achilles_results <- achillesResults.csv

	achillesResultsAugmented.csv <- read.csv(paste0(cdmReleaseFolder,"/achilles_results_augmented.csv"), stringsAsFactors = F)
	achillesResultsAugmented.csv$DATABASE_ID <- databaseId
	achillesResultsAugmented.csv$DB_ID <- dbId
	dp_achilles_results_augmented <- achillesResultsAugmented.csv
	
	dp_achilles_results$STRATUM_1 <- as.character(dp_achilles_results$STRATUM_1)
	dp_achilles_results$STRATUM_2 <- as.character(dp_achilles_results$STRATUM_2)
	dp_achilles_results$STRATUM_3 <- as.character(dp_achilles_results$STRATUM_3)
	dp_achilles_results$STRATUM_4 <- as.character(dp_achilles_results$STRATUM_4)
	dp_achilles_results$STRATUM_5 <- as.character(dp_achilles_results$STRATUM_5)

	dp_achilles_results_augmented$STRATUM_1 <- as.character(dp_achilles_results_augmented$STRATUM_1)
	dp_achilles_results_augmented$STRATUM_2 <- as.character(dp_achilles_results_augmented$STRATUM_2)
	dp_achilles_results_augmented$STRATUM_3 <- as.character(dp_achilles_results_augmented$STRATUM_3)
	dp_achilles_results_augmented$STRATUM_4 <- as.character(dp_achilles_results_augmented$STRATUM_4)
	dp_achilles_results_augmented$STRATUM_5 <- as.character(dp_achilles_results_augmented$STRATUM_5)
	dp_achilles_results_augmented$VISIT_ANCESTOR_CONCEPT_ID <- as.character(dp_achilles_results_augmented$VISIT_ANCESTOR_CONCEPT_ID)

	tablesToUpload <- c("dp_achilles_results","dp_achilles_results_augmented")
	
	conn <- DatabaseConnector::connect(uploadConnectionDetails)

	for (tableName in tablesToUpload) {
	  DatabaseConnector::insertTable(
		connection        = conn,
		tableName         = tableName,
		databaseSchema    = uploadResultsSchema, 
		data              = eval(parse(text=tableName)),
		dropTableIfExists = FALSE,
		createTable       = FALSE,
		tempTable         = FALSE,
		progressBar       = TRUE)
	}

	DatabaseConnector::disconnect(conn)
}

# Upload DQD results to upload results schema
uploadDQDResults <- function(
	uploadConnectionDetails,
	uploadResultsSchema,
	cdmDatabaseSchema,
	outputFolder,
	dbId,
	databaseId) 
{
	cdmReleaseName   <- substr(cdmDatabaseSchema,5,nchar(cdmDatabaseSchema))
	cdmReleaseFolder <- paste(outputFolder,cdmReleaseName,sep = "/")
	
	dqdJsonDf <- jsonlite::fromJSON(
	  paste0(cdmReleaseFolder,"/",dbId,"_DbProfile.json"),
	  simplifyDataFrame = TRUE)

	dp_overview     <- as.data.frame(dqdJsonDf$Overview)
	dp_checkresults <- as.data.frame(dqdJsonDf$CheckResults)
	dp_metadata     <- as.data.frame(dqdJsonDf$Metadata)
	dp_overview$DATABASE_ID      <- databaseId
	dp_checkresults$DATABASE_ID  <- databaseId
	dp_metadata$DATABASE_ID      <- databaseId
	dp_overview$DB_ID      <- dbId
	dp_checkresults$DB_ID  <- dbId
	dp_metadata$DB_ID      <- dbId
	dp_checkresults$THRESHOLD_VALUE <- as.character(dp_checkresults$THRESHOLD_VALUE)
	
	tablesToUpload <- c("dp_checkresults","dp_metadata","dp_overview")
	
	conn <- DatabaseConnector::connect(uploadConnectionDetails)

	for (tableName in tablesToUpload) {
	  DatabaseConnector::insertTable(
		connection        = conn,
		tableName         = tableName,
		databaseSchema    = uploadResultsSchema, 
		data              = eval(parse(text=tableName)),
		dropTableIfExists = FALSE,
		createTable       = FALSE,
		tempTable         = FALSE,
		progressBar       = TRUE)
	}

	DatabaseConnector::disconnect(conn)
}