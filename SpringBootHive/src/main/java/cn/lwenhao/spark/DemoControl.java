package cn.lwenhao.spark;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/spark")
public class DemoControl {
	@Autowired
	private SparkTestService sparkTestService;
	
	
	
	@RequestMapping("/demo/hdfs")
	public Map<String, Object> sparkDemo() {
		System.out.println("/demo/hdfs");
		return sparkTestService.sparkDemo();
	}
	
	@RequestMapping("/demo/local")
	public Map<String, Object> sparkMysql() {
		System.out.println("/demo/local");
		return sparkTestService.sparkLocal();
	}
	
	
//	@RequestMapping(value="/job/executeJob",method=RequestMethod.GET)
//    @ResponseBody
//    RetMsg executeSparkJob(@RequestParam("jarId") String jarId,@RequestParam("sparkUri") String sparkUri) {
//        RetMsg ret = new RetMsg();
//        StringBuffer msg = new StringBuffer(jarId+":"+sparkUri);
//        ret.setMsg(msg.toString());
//        SparkJobLog jobLog = new SparkJobLog();
//        jobLog.setExecTime(new Date());
//        String[] arg0=new String[]{
//                "/usr/job/"+jarId,
//                "--master",sparkUri,   
//                "--name","web polling",   
//                "--executor-memory","1G"
//        };
//        log.info("提交作业...");
//        try {
//            SparkSubmit.main(arg0);
//        } catch (Exception e) {
//            log.info("出错了！", e);
//            ret.setCode(1);
//            ret.setMsg(e.getMessage());
//            msg.append(e.getMessage());
//        }
//        jobLog.setMsg(msg.toString());
//        sparkJobLogService.insertLog(jobLog);
//        return ret;
//    }
}
