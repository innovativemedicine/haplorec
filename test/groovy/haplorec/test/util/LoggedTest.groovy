package haplorec.test.util

class LoggedTest {

    StackTraceElement stackframe(Map kwargs = [:]) {
        if (kwargs.above == null) { kwargs.above = 'stackframe' }
        def matcher = { arg ->
            if (arg instanceof Closure) {
                return arg
            } else {
                return { frame ->
                    frame.methodName == arg
                }
            }
        }
        // return the stackframe that called this method
        def frames = Thread.currentThread().getStackTrace()
        def frameMatches
        if (kwargs.matches != null) { 
            frameMatches = matcher(kwargs.matches)
            def i = 0
            while (!frameMatches(frames[i])) {
                i += 1
            }
            return frames[i]
        } else {
            // use kwargs.above
            frameMatches = matcher(kwargs.above)
            def i = 0
            while (!frameMatches(frames[i])) {
                i += 1
            }
            return frames[i + 1]
        }
    }

    void logit(Map kwargs = [:], msg) {
        if (kwargs.level == null) { kwargs.level = 'info' }
        def frame = stackframe(matches: { frame -> frame.methodName =~ /^test/ })
        def message = frame.toString() + ': ' + msg
        if (kwargs.level == 'info') {
            log.info(message)
        }
    }

}
