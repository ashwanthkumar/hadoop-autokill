package in.ashwanthkumar.hacks.hadoop;

public class HumanReadableJobStatus {
	public static String getHumanReadableJobStatus(int status) {
		switch (status) {
		case 1:
			return "RUNNING";
		case 2:
			return "SUCCEDED";
		case 3:
			return "FAILED";
		case 4:
			return "PREP";
		case 5:
			return "KILLED";

		default:
			return "N/A";
		}
	}
}
