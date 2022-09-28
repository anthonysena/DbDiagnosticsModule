# Copyright 2022 Observational Health Data Sciences and Informatics
#
# This file is part of DbProfileModule
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

#' Create specifications for the DbProfileModule
#'
#' @param connectionDetails         A connectionDetails object for connecting to the database containing the DbProfile results
#'                    
#' @param resultsDatabaseSchema     The fully qualified database name of the results schema where the DbProfile results are housed
#' 
#' @param resultsTableName					The name of the table in the results schema with the DbProfile results
#'                    
#' @param dataDiagnosticsSettings		A list of settings created from DataDiagnostics::createDataDiagnosticsSettings() function
#'                    
#'
#' @return
#' An object of type `DbDiagnosticsModuleSpecifications`.
#'
#' @export
createDbDiagnosticsModuleSpecifications <- function(connectionDetails,
                                                resultsDatabaseSchema,
                                                resultsTableName,
                                                dataDiagnosticsSettings) {
  analysis <- list()
  for (name in names(formals(createDbDiagnosticsModuleSpecifications))) {
    analysis[[name]] <- get(name)
  }
  
  specifications <- list(module = "%module%",
                         version = "%version%",
                         remoteRepo = "github.com",
                         remoteUsername = "ohdsi",
                         settings = analysis)
  class(specifications) <- c("DbDiagnosticsModuleSpecifications", "ModuleSpecifications")
  return(specifications)
}
