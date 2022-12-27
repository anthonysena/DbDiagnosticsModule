# Copyright 2022 Observational Health Data Sciences and Informatics
#
# This file is part of dbDiagnosticsModule
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
  
  # TODO add a check to make sure the table dp_achilles_results_augmented exists

  if (is.null(jobContext$settings)) {
    stop("Analysis settings not found in job context")
  }
  if (is.null(jobContext$sharedResources)) {
    stop("Shared resources not found in job context")
  }
  if (is.null(jobContext$moduleExecutionSettings)) {
    stop("Execution settings not found in job context")
  }
  
  resultsSubFolder <- jobContext$moduleExecutionSettings$resultsSubFolder
  
  rlang::inform("Executing")

  DbDiagnostics::executeDbDiagnostics(
    connectionDetails       = jobContext$moduleExecutionSettings$resultsConnectionDetails, #this is here because I need to connect to the results database to get the dbProfile results
    resultsDatabaseSchema   = jobContext$moduleExecutionSettings$resultsDatabaseSchema,
    resultsTableName        = "dp_achilles_results_augmented",
    outputFolder            = resultsSubFolder,
    dataDiagnosticsSettings = jobContext$settings$dataDiagnosticsSettings
  )

  rlang::inform("Zipping up results")

  # Set the table names in resultsDataModelSpecification.csv
  moduleInfo <- getModuleInfo()
  resultsDataModel <- CohortGenerator::readCsv(
    file = "resultsDataModelSpecification.csv",
    warnOnCaseMismatch = FALSE
  )
  newTableNames <- paste0(moduleInfo$TablePrefix, resultsDataModel$tableName)
  file.rename(
    file.path(resultsSubFolder, paste0(unique(resultsDataModel$tableName), ".csv")),
    file.path(resultsSubFolder, paste0(unique(newTableNames), ".csv"))
  )
  resultsDataModel$tableName <- newTableNames
  CohortGenerator::writeCsv(
    x = resultsDataModel,
    file = file.path(resultsSubFolder, "resultsDataModelSpecification.csv"),
    warnOnCaseMismatch = FALSE,
    warnOnFileNameCaseMismatch = FALSE,
    warnOnUploadRuleViolations = FALSE
  )
  
  # Zip the results 
  zipFile <- file.path(resultsSubFolder, "dbDiagnosticsResults.zip")
  resultFiles <- list.files(resultsSubFolder,
                            pattern = ".*\\.csv$"
  )
  oldWd <- setwd(resultsSubFolder)
  on.exit(setwd(oldWd), add = TRUE)
  DatabaseConnector::createZipFile(
    zipFile = zipFile,
    files = resultFiles
  )
  rlang::inform(paste("Results available at:", zipFile))
}

# Private methods -------------------------
getModuleInfo <- function() {
  checkmate::assert_file_exists("MetaData.json")
  return(ParallelLogger::loadSettingsFromJson("MetaData.json"))
}
