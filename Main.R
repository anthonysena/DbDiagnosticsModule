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
  
  resultsFolder <- jobContext$moduleExecutionSettings$resultsFolder
  
  rlang::inform("Executing")

  DbDiagnostics::executeDbDiagnostics(
    connectionDetails       = jobContext$moduleExecutionSettings$resultsConnectionDetailsReference, #this is here because I need to connect to the results database to get the dbProfile results
    resultsDatabaseSchema   = jobContext$moduleExecutionSettings$resultsDatabaseSchema,
    resultsTableName        = "dp_achilles_results_augmented",
    outputFolder            = resultsFolder,
    dataDiagnosticsSettings = jobContext$settings$dataDiagnosticsSettings
  )

  rlang::inform("Zipping up results")

  # Copy in the resultsDataModelSpecification.csv
  # TODO --------------------
  # The file names are dynamic, how does that play into the specification?
  file.copy(from = "resultsDataModelSpecification.csv",
            to = file.path(resultsFolder, "resultsDataModelSpecification.csv"))
  
  # Zip the results 
  zipFile <- file.path(resultsFolder, "dbDiagnosticsResults.zip")
  resultFiles <- list.files(resultsFolder,
                            pattern = ".*\\.csv$"
  )
  oldWd <- setwd(resultsFolder)
  on.exit(setwd(oldWd), add = TRUE)
  DatabaseConnector::createZipFile(
    zipFile = zipFile,
    files = resultFiles
  )
  rlang::inform(paste("Results available at:", zipFile))
}
