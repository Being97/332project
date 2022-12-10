package WorkerTool

trait MASTERSTATE
case object MASTERREADY extends MASTERSTATE
case object CONNECTED extends MASTERSTATE
case object PIVOTED extends MASTERSTATE
case object PARTITIONED extends MASTERSTATE
case object FAILED extends MASTERSTATE

trait WORKERSTATE
case object WORKERREADY extends WORKERSTATE
case object SAMPLED extends WORKERSTATE
