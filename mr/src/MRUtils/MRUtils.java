package MRUtils;

import java.util.HashMap;
import java.util.Map;

public class MRUtils {

	// This helper function parses the stackoverflow into a Map for us.
	public static Map<String, String> transformXmlToMap(String xml) {
		Map<String, String> map = new HashMap<String, String>();
		try {
			String[] tokens = xml.trim().substring(5, xml.trim().length() - 3)
					.split("\"");

			for (int i = 0; i < tokens.length - 1; i += 2) {
				String key = tokens[i].trim();
				String val = tokens[i + 1];

				map.put(key.substring(0, key.length() - 1), val);
			}
		} catch (StringIndexOutOfBoundsException e) {
			System.err.println(xml);
		}

		return map;
	}

	public static Map<String, String> transformCSVToMap(String[] header, String csv) {
		Map<String, String> map = new HashMap<String, String>();
		try {
			int i = 0;
			String[] allData = csv.trim().split(",");
			if(header[0].equals(allData[0])){
				map.put(null, null);
				return map;
			}

//			@SuppressWarnings("unused")
//			String s = null;
//			boolean flag = false;
			for(String Data : allData)
			{
//				if(Data.charAt(0) == '"' || flag){
//					s += Data;
//					if(Data.charAt(Data.length() - 1) != '"')
//						flag = true;
//					else
//						flag = false;
//				}
//				if(!flag){
					map.put(header[i], Data);
					i++;
//				}
			}

		} catch (Exception e) {
			System.err.println(csv);
		}

		return map;
	}
}