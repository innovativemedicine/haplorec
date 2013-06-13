package haplorec.test.util

@Mixin(LoggedTest)
class TimedTest {

    def shouldRunWithin(Map kwargs = [:], Closure test) {
        def timelimit
        if (kwargs.seconds != null) { timelimit = kwargs.seconds * 1000 }
        else if (kwargs.milliseconds != null) { timelimit = kwargs.milliseconds }
        else if (kwargs.minutes != null) { timelimit = kwargs.minutes * 60 * 1000 }
        if (kwargs.logTime == null) { kwargs.logTime = true }
        def start = System.currentTimeMillis()
        test()
        def end = System.currentTimeMillis()
        def executionTime = end - start
        if (kwargs.logTime) {
            logit "test took ${executionTime/1000} seconds", level: 'info'
        }
        assert executionTime <= timelimit
    }

}
