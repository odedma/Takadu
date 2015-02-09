import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.junit.Test;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;


public class WaterNetworkDataProcessor {
	
    private static final ThreadLocal<DateFormat> DF = new ThreadLocal<DateFormat>(){
    	private static final String DATE_FORMAT = "dd/MM/yyyy";
		@Override
		protected DateFormat initialValue() {
			return new SimpleDateFormat(DATE_FORMAT);
		}
	};			
	
	// HEADERS
	private static final String C_HEADER_SUPPLY_ZONE = "supply zone";           // Supply values.csv
	private static final String C_HEADER_DATE = "date";                         // Supply values.csv
	private static final String C_HEADER_SUPPLY = "supply (Ml/d)";              // Supply values.csv
	
	private static final String C_HEADER_ZONE_NAME = "zone name";               // Supply zones.csv
	private static final String C_HEADER_PARENT_ZONE_NAME = "parent zone name"; // Supply zones.csv
		
	// TYPE OF VALUES
	private static final String C_TYPE_ACTUAL = "actual";
	private static final String C_TYPE_AGGREGATED = "aggregated";
	
	private static final String C_FULL_PATH_DIR = "d:" + File.separator;
	private static final String C_SV_FILE_NAME = "Supply values.csv";
	private static final String C_SZ_FILE_NAME = "Supply zones.csv";
	private static final String C_OUTPUT_FILE_NAME = "output.json";
	
	private static final File SUPPLY_VALUES_CSV_FILE = new File(C_FULL_PATH_DIR + C_SV_FILE_NAME);
	private static final File SUPPLY_ZONES_CSV_FILE = new File(C_FULL_PATH_DIR + C_SZ_FILE_NAME);
	private static final File OUTPUT_JSON_FILE = new File(C_FULL_PATH_DIR + C_OUTPUT_FILE_NAME);
	
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	
	// Supporting Data Structures for Data processing   
	private Map<String, Map <String , String>> supplyValues = null;
	private SortedSet<String> dates = null;
	private Map <String , Set<String>> directChildren = null;
	
	// Read From input files
	private List<Map<String, String>> inputData = null;
	
	//Write to output file
	List<OutputEntity> outputData = null;
	
	@Test
	public void testWaterNetwork() {
		populateSupportingDataStructures();
		processData();
		writeOutputFile();
	}

	private void populateSupportingDataStructures() {		
		collectDataFromSupplyValues();
		collectDataFromSupplyZones();	
	}
	
	private void collectDataFromSupplyValues() {
		String zone = null, date = null, supply = null;
		Map <String , String> dateSupplyMap = null;
		try {
			inputData = readObjectsFromCsv(SUPPLY_VALUES_CSV_FILE);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		if (inputData != null && inputData.size() > 0) {
			supplyValues = new HashMap<String, Map <String , String>>();
			dates = new TreeSet<String>(new Comparator<String> () {
				@Override
				public int compare(String dateStr1, String dateStr2) {
					Date date1=null, date2=null;
					try {
						date1 = DF.get().parse(dateStr1);
						date2 = DF.get().parse(dateStr2);
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					return date1.compareTo(date2);
				}
			});			
			for (Map<String, String> row : inputData) {
				if (row != null && !row.isEmpty()) {
					zone = row.get(C_HEADER_SUPPLY_ZONE);
					date = row.get(C_HEADER_DATE);
					supply = row.get(C_HEADER_SUPPLY);
					dateSupplyMap = supplyValues.get(zone);
					if (dateSupplyMap == null) {
						dateSupplyMap = new HashMap<String,String>();
						supplyValues.put(zone, dateSupplyMap);
					}
					dateSupplyMap.put(date, supply);
					dates.add(date);
				}
			}
		}		
	}

	private void collectDataFromSupplyZones() {
		String zone = null, parent = null;
		try {
			inputData = readObjectsFromCsv(SUPPLY_ZONES_CSV_FILE);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		if (inputData != null && inputData.size() > 0) {		
			for (Map<String, String> row : inputData) {
				if (row != null && !row.isEmpty()) {
					zone = row.get(C_HEADER_ZONE_NAME);
					parent = row.get(C_HEADER_PARENT_ZONE_NAME);					
					if (directChildren == null) {
						directChildren = new HashMap<String,Set<String>>();
					}					
					if (!directChildren.containsKey(zone)){
						directChildren.put(zone, new HashSet<String>());
					}					
					if ((parent!=null) && !(parent.isEmpty())) {
						if (!directChildren.containsKey(parent)){
							directChildren.put(parent, new HashSet<String>());
						}						
						directChildren.get(parent).add(zone);							
					}
				}
			}
		}
	}



	public static List<Map<String, String>> readObjectsFromCsv(File file) throws IOException {
		CsvSchema bootstrap = CsvSchema.emptySchema().withHeader();
		CsvMapper csvMapper = new CsvMapper();
		MappingIterator<Map<String, String>> mappingIterator = csvMapper.reader(Map.class).with(bootstrap).readValues(file);
		return mappingIterator.readAll();
	}	
	
	private void processData() {
		outputData = new ArrayList<OutputEntity>();
		for (String strDate : dates) {
			if ( (strDate != null) && !(strDate.isEmpty()) ) {
				for(Map.Entry<String,Set<String>> entry : directChildren.entrySet()) {
					if (entry != null) {
						String p = entry.getKey();
						String value = null;
						if ((p != null) && 
							!(p.isEmpty()) &&
							(supplyValues.get(p) != null)  &&
							(supplyValues.get(p).get(strDate) != null) &&
							!(supplyValues.get(p).get(strDate).isEmpty()) ) {								
							value = supplyValues.get(p).get(strDate);
							outputData.add(new OutputEntity(p,strDate,value,C_TYPE_ACTUAL));
						} else {
							Set<String> dc = entry.getValue();
							int countValues = 0;
							int val = 0;
							for (String c : dc) {
								if ((c != null) && 
									!(c.isEmpty()) &&
									(supplyValues.get(c) != null)  &&
									(supplyValues.get(c).get(strDate) != null) &&
									!(supplyValues.get(c).get(strDate).isEmpty()) ) {										

									String cvalue = supplyValues.get(c).get(strDate);
									if ( (cvalue != null) && !(cvalue.isEmpty()) ) {
										val += Integer.parseInt(cvalue);
										countValues++;
									}
									if (countValues == dc.size()) {
										value = String.valueOf(val);
										outputData.add(new OutputEntity(p,strDate,value,C_TYPE_AGGREGATED));
									}									
								}								
							}
						}							
					}
				}				
			}
		}	
	}	

	private void writeOutputFile() {
		try {

			writeAsJson(outputData, OUTPUT_JSON_FILE);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	
	public static void writeAsJson(List<OutputEntity> outputData, File file) throws IOException {
		OBJECT_MAPPER.writeValue(file, outputData);
	}
	
	@JsonPropertyOrder({ "zoneName", "date", "value", "type" })
	public class OutputEntity { 
		
		private String zoneName;
		private String date;
		private String value;
		private String type;
		
		public OutputEntity(String zoneName, String date, String value, String type) {
			this.zoneName = zoneName;
			this.date = date;
			this.value = value;
			this.type = type;
		}
		
		public String getZoneName() {
			return zoneName;
		}
		public void setZoneName(String zoneName) {
			this.zoneName = zoneName;
		}

		public String getDate() {
			return date;
		}
		public void setDate(String date) {
			this.date = date;
		}

		public String getValue() {
			return value;
		}
		public void setValue(String value) {
			this.value = value;
		}

		public String getType() {
			return type;
		}
		public void setType(String type) {
			this.type = type;
		}
	}
}
