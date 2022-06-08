createDbProfileModuleSpecifications <- function() 
{

  checkmate::assert_file_exists("MetaData.json")
  moduleInfo <- ParallelLogger::loadSettingsFromJson("MetaData.json")
  
  specifications <- list(module = moduleInfo$Name,
                         version = moduleInfo$Version,
                         remoteRepo = "github.com",
                         remoteUsername = "ohdsi")
						 
  class(specifications) <- c("DbProfileModuleSpecifications", "ModuleSpecifications")
  return(specifications)
}