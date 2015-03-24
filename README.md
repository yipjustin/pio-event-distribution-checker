# Event Distribution Checker

It prints out the landscape of an appid.

## To run

edit engine.json. 2 key fields.

- appId: the appId
- sample: the # of event to be printed for each key. can be empty.

```
$ pio build
$ pio train
...
[INFO] [BasicChecker] Event (Time, Name, EntityType, TargetEntityType) Distribution
[INFO] [AggEvent$] [1 x 4]
                               Count   Freq CumCount CumFreq 
                            -------- ------ -------- ------- 
(2015-03,$set,user,None) -> 153.0000 1.0000 153.0000  1.0000 
...
[INFO] [CoreWorkflow$] Training interrupted by io.prediction.workflow.StopAfterReadInterruption.
```

