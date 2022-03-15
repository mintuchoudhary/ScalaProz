import io.cucumber.junit.{Cucumber, CucumberOptions}
import org.junit.runner.RunWith

@RunWith(classOf[Cucumber])
@CucumberOptions(tags = {
  "@firstFeature or @secondFeature or @dataframeFeature"
})
class TestRunner {}

