package cn.lwenhao.test;

public class TestString {

	public static void main(String[] args) {
		String a = "2019-02-27T02:57:32.813569Z	   54 Query	INSERT INTO his_agent_status(`agentname`,`sys_cpu`,`sys_mem`,`sys_disk`,`net_name`,`net_input`,`net_output`,`ctime`) VALUES ('agent01',74.46,37.58,57.46,'TestFlume',70.98,0,'2019-02-27 10:57:11')";
	
		String[] temp_arr = a.split("VALUES");
		
		String temp = temp_arr[1].substring(1);
		
		String[] values = temp.split(",");
		
		
		System.out.println(temp);
	
		System.out.println(values[0].substring(1));
	
		System.out.println(values[values.length-1].substring(0,values[values.length-1].length()-1));
	}
	
	
}
