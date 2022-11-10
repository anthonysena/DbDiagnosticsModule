# Create a job context for testing purposes
# remotes::install_github("OHDSI/Strategus", ref="develop")
library(Strategus)
library(dplyr)
library(DbDiagnostics)
source("SettingsFunctions.R")

# Generic Helpers ----------------------------
getModuleInfo <- function() {
  checkmate::assert_file_exists("MetaData.json")
  return(ParallelLogger::loadSettingsFromJson("MetaData.json"))
}

# Create DbDianosticsModule settings ---------------------------------------

dbDiagnosticsSettings <- DbDiagnostics::createDataDiagnosticsSettings()

dbDiagnosticsModuleSpecifications <- createDbDiagnosticsModuleSpecifications(
  # connectionDetails = "dummy",
  # resultsDatabaseSchema = "dummy",
  # resultsTableName = "dummyTable",
  dataDiagnosticsSettings = dbDiagnosticsSettings
)

# Module Settings Spec ----------------------------
analysisSpecifications <- createEmptyAnalysisSpecificiations() %>%
  addModuleSpecifications(dbDiagnosticsModuleSpecifications)

executionSettings <-   Strategus::createResultsExecutionSettings(
  resultsConnectionDetailsReference = "dummy",
  resultsDatabaseSchema = "dummy",
  workFolder = "dummy",
  resultsFolder = "dummy",
  minCellCount = 5
)

# Job Context ----------------------------
module <- "DbDiagnosticsModule"
moduleIndex <- 1
moduleExecutionSettings <- executionSettings
moduleExecutionSettings$workSubFolder <- "dummy"
moduleExecutionSettings$resultsSubFolder <- "dummy"
moduleExecutionSettings$databaseId <- 123
jobContext <- list(
  sharedResources = analysisSpecifications$sharedResources,
  settings = analysisSpecifications$moduleSpecifications[[moduleIndex]]$settings,
  moduleExecutionSettings = moduleExecutionSettings
)
saveRDS(jobContext, "tests/testJobContext.rds")
