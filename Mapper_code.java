package value_first_map_reduce1;


import java.io.IOException;
import java.time.Year;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class Mapper_code extends Mapper < LongWritable, Text, Text, Text> {
  String attributeValue = " ";
  String attributeName = " ";
  String public_attribute = " "; 
  double priority;
  
  public void map(LongWritable key, Text value, Context context) throws IOException {
   String line;
   String smsText;
   String smsId;
   String time;
   String phoneNo;
   String userid;
   String fromNumber;
   String circle;
   String usercategory;
   String operator;
     
   String sms[] = new String[20];
   

   String smsLower;

   Mapper_code objMapper = new Mapper_code();

   line = value.toString();
   try {
	   sms = line.split("\\#\\|");

    smsText = sms[13];
    smsId = sms[0];
    time = sms[10];
    phoneNo = sms[3];
    userid = sms[2];
    fromNumber = sms[4];
    circle = sms[9];
    operator=sms[8];
    usercategory=sms[12];

    smsLower = smsText.toLowerCase() + " ";
   
  String an="",a="",b="",c="",d="",e="",f="",g="",h="",i="",j="",k="",l="",m="",n="",o="",p="",q="",r="",s="",t="",u="",v="",w="",x="",y="",z="",aa="",ab="",ac="",ad="",ae="",af="",ag="",ah="",ai="",aj="",ak="",al="",am="";
	   
	    //exclude promotional sms's .. user_category 3 is promotional sms.
	    	if (!"3".equals(sms[6]) && !"2".equals(sms[6])){
	    		objMapper.location(smsLower, smsId, phoneNo, userid, time, context,fromNumber,operator,circle,usercategory);
			    objMapper.credit_card(smsLower, smsId, phoneNo, userid, time, context,operator,circle,usercategory,fromNumber);
			    objMapper.emailid(smsLower, smsId, phoneNo, userid, time, context,operator,circle,usercategory,fromNumber);
			    objMapper.premium_sum(smsLower, smsId, phoneNo, userid, time, context, operator, circle, fromNumber,usercategory);
			    objMapper.life_insurance(smsLower, smsId, phoneNo, userid, time, fromNumber, context,operator,circle,usercategory);
			    objMapper.health_insurance(smsLower, smsId, phoneNo, userid, time, fromNumber,usercategory, context,operator,circle);
			    objMapper.premium_date(smsLower, smsId, phoneNo, userid, time, context,operator,circle,usercategory,fromNumber);
			    objMapper.mutual_fund(smsLower, smsId, phoneNo, userid, time, context,operator,circle,usercategory,fromNumber);
			    objMapper.home_loan(smsLower, smsId, phoneNo, userid, time, fromNumber, context,operator,circle,usercategory);
			    objMapper.internet_banking(smsLower, smsId, phoneNo, userid, time, context,operator,circle,usercategory,fromNumber);
			    objMapper.creditcard_limit(smsLower, smsId, phoneNo, userid, time, context,operator,circle,usercategory,fromNumber);
			    objMapper.gender(smsLower, smsId, phoneNo, userid, time, context,fromNumber,operator,circle,usercategory);
			    objMapper.has_kids(smsLower, smsId, phoneNo, userid, time, fromNumber, context,operator,circle,usercategory);
			    objMapper.car_loan(smsLower, smsId, phoneNo, userid, time, fromNumber, context,operator,circle,usercategory);
			    objMapper.car_emi(smsLower, smsId, phoneNo, userid, time, fromNumber, context,operator,circle,usercategory);
			    objMapper.car_emiduedate(smsLower, smsId, phoneNo, userid, time, fromNumber, context,operator,circle,usercategory);
			    objMapper.home_loanemi(smsLower, smsId, phoneNo, userid, time, fromNumber, context,operator,circle,usercategory);
			    objMapper.home_emiduedate(smsLower, smsId, phoneNo, userid, time, fromNumber, context,operator,circle,usercategory);
			    objMapper.lifeinsurance_emi(smsLower, smsId, phoneNo, userid, time, fromNumber,context,operator,circle,usercategory);
			    objMapper.lifeinsurance_emiduedate   (smsLower,smsId,phoneNo,userid,time,fromNumber,context,operator,circle,usercategory);
			    objMapper.healthinsurance_emi(smsLower, smsId, phoneNo, userid, time, fromNumber, context,operator,circle,usercategory);
			    objMapper.healthinsurance_emiduedate(smsLower, smsId, phoneNo, userid, time, fromNumber, context,operator,circle,usercategory);
			    objMapper.carinsurance_emi(smsLower, smsId, phoneNo, userid, time, fromNumber, context,operator,circle,usercategory);
			    objMapper.carinsurance_emiduedate(smsLower, smsId, phoneNo, userid, time, fromNumber, context,operator,circle,usercategory);
	    		objMapper.age(smsLower, smsId, phoneNo, userid, time, context,fromNumber,operator,circle,usercategory);
	    		objMapper.income(smsLower, smsId, phoneNo, userid, time, fromNumber, context,operator,circle,usercategory);
			    objMapper.dob(smsLower, smsId, phoneNo, userid, time, context,fromNumber,operator,circle,usercategory);
			    objMapper.username(smsLower, smsId, phoneNo, userid, time, context,fromNumber,operator,circle,usercategory);
			    objMapper.apparel(smsLower, smsId, phoneNo, userid, time, context,operator,circle,fromNumber,usercategory);
			    objMapper.gadgets(smsLower, smsId, phoneNo, userid, time, context,operator,circle,fromNumber,usercategory);
			    objMapper.sports(smsLower, smsId, phoneNo, userid, time, context,operator,circle,fromNumber,usercategory);
			    objMapper.movies(smsLower, smsId, phoneNo, userid, time, fromNumber, context,operator,circle,usercategory);
			    objMapper.frequent_traveller(smsLower, smsId, phoneNo, userid, time, fromNumber, context,operator,circle,usercategory);
			    objMapper.frequent_shopper(smsLower, smsId, phoneNo, userid, time, fromNumber, context,operator,circle,usercategory);
			    objMapper.car_insurance(smsLower, smsId, phoneNo, userid, time, fromNumber, context,operator,circle,usercategory);
			    objMapper.ecom_expense(smsLower, smsId, phoneNo, userid, time, fromNumber, context,operator,circle,usercategory);
			    objMapper.withdrawl(smsLower, smsId, phoneNo, userid, time,fromNumber, context,operator,circle,usercategory);
			    objMapper.credited(smsLower, smsId, phoneNo, userid, time, context,operator,circle,fromNumber,usercategory);
			    objMapper.brand_shopper(smsLower, smsId, phoneNo, userid, time, context,operator,circle,fromNumber,usercategory);
			    objMapper.savings_account(smsLower, smsId, phoneNo, userid, time, context,operator,circle,fromNumber,usercategory);
	    		
			 
	    }
	      } catch (Exception ex) {
	    	  	
	    	    ex.printStackTrace();
	    
   }
 }
  
  public String location(String smstxt, String smsId, String phoneNo,String userid, String time, Context context, String fromNumber,String operator,String circle, String usercategory) throws IOException {
	   Map < String, List < String >> map = new HashMap < String, List < String >> ();


	   
	   priority=0;
	   List < String > maharashtra = new ArrayList < String > ();
	   maharashtra.add("pune");
	   maharashtra.add("nagpur");
	   maharashtra.add("ahmednagar");
	   maharashtra.add("akola");
	   maharashtra.add("amravati");
	   maharashtra.add("aurangabad");
	   maharashtra.add("beed");
	   maharashtra.add("bhandara");
	   maharashtra.add("buldana");
	   maharashtra.add("palghar");
	   maharashtra.add("wardha");
	   maharashtra.add("solapur");
	   maharashtra.add("satara");
	   maharashtra.add("nashik");
	   maharashtra.add("latur");
	   maharashtra.add("kolhapur");
	   maharashtra.add("jalna");
	   maharashtra.add("jalgaon");
	   maharashtra.add("hingoli");
	   maharashtra.add("gondia");
	   maharashtra.add("gadchiroli");
	   maharashtra.add("dhule");
	   maharashtra.add("chandrapur");
	  


	   List < String > rajasthan = new ArrayList < String > ();
	   rajasthan.add("udaipur");
	   rajasthan.add("jodhpur");
	   rajasthan.add("jaisalmer");
	   rajasthan.add("jaipur");
	   rajasthan.add("ajmer");
	   rajasthan.add("bikaner");
	   rajasthan.add("kota");
	   rajasthan.add("alwar");
	   rajasthan.add("bharatpur");
	   rajasthan.add("dholpur");
	   rajasthan.add("rajsamand");
	   rajasthan.add("pali");
	   rajasthan.add("sikar");
	   rajasthan.add("sirohi");
	   rajasthan.add("kishangarh");
	   rajasthan.add("nasira bad");
	   rajasthan.add("beawar");
	   rajasthan.add("nasirabad");
	   rajasthan.add("sarwar");
	   rajasthan.add("kekri");
	   rajasthan.add("bhinai");
	   rajasthan.add("pisangan");
	   rajasthan.add("masuda");
	   rajasthan.add("beawar");
	 
	   
	   List < String > haryana = new ArrayList < String > ();
	   haryana.add("ambala");
	   haryana.add("bhiwani");
	   haryana.add("fatehabad");
	   haryana.add("hisar");
	   haryana.add("jhajjar");
	   haryana.add("jind");
	   haryana.add("kaithal");
	   haryana.add("karnal");
	   haryana.add("kurukshetra");
	   haryana.add("mahendragarh");
	   haryana.add("palwal");
	   haryana.add("panipat");
	   haryana.add("rohtak");
	   haryana.add("sonipat");
	   
	     
	   List < String > himachal = new ArrayList < String > ();
	   himachal.add("bilaspur");
	   himachal.add("chamba");
	   himachal.add("hamirpur");
	   himachal.add("kangra");
	   himachal.add("kinnaur");
	   himachal.add("kullu");
	   himachal.add("mandi");
	   himachal.add("shimla");
	   himachal.add("solan");
	   himachal.add("unna");
	   
	   List < String > karnataka = new ArrayList < String > ();
	   karnataka.add("bangalore");
	   karnataka.add("mysore");
	   karnataka.add("ooty");
	   karnataka.add("bangaluru");
	   karnataka.add("bangalkot");
	   karnataka.add("bellary");
	   karnataka.add("vijayapura");
	   karnataka.add("dharwad");
	   karnataka.add("gadag");
	   karnataka.add("kalaburagi");
	   karnataka.add("hassan");
	   karnataka.add("haveri");
	   karnataka.add("kodagu");
	   karnataka.add("kolar");
	   karnataka.add("koppal");
	   karnataka.add("mandya");
	   karnataka.add("mysuru");
	   karnataka.add("raichur");
	   

	   List < String > bihar = new ArrayList < String > ();
	   bihar.add("patna");
	   bihar.add("muzzafarnagar");
	   bihar.add("ranchi");
	   bihar.add("khunti");
	   bihar.add("chatra");
	   bihar.add("samastipur");
	   bihar.add("saharsa");
	   bihar.add("rohtas");
	   bihar.add("purnia");
	   bihar.add("nawada");
	   bihar.add("nalanda");
	   bihar.add("muzaffarpur");
	   bihar.add("madhepura");
	   bihar.add("munger");
	   bihar.add("madhubani");
	   bihar.add("lakhisarai");
	   bihar.add("katihar");
	   bihar.add("kaimur");
	   bihar.add("kishanganj");
	   bihar.add("khagaria");
	   bihar.add("jehanabad");
	   bihar.add("jamui");
	   bihar.add("gopalganj");
	   bihar.add("gaya");
	   bihar.add("east champaran");
	   bihar.add("darbhanga");
	   bihar.add("buxar");
	   bihar.add("bhojpur");
	   bihar.add("bhagalpur");
	   bihar.add("begusarai");
	   bihar.add("banka");
	   bihar.add("aurangabad");
	   bihar.add("arwal");
	   bihar.add("araria");
	  
	   List < String > northeast = new ArrayList < String > ();

	   northeast.add("arunachal pradesh");
	   northeast.add("meghalaya");
	   northeast.add("mizoram");
	   northeast.add("manipur");
	   northeast.add("nagaland");
	   northeast.add("tripura");
	   
	   northeast.add("imphal west");
	   northeast.add("churachandpur");
	   northeast.add("chandel");
	   northeast.add("thoubal");
	   northeast.add("tamenglong");
	   northeast.add("ukhrul");
	   northeast.add("imphal east");
	   northeast.add("bishnupur");
	   northeast.add("senapati");
	   northeast.add("aizawl");
	   northeast.add("mammit");
	   northeast.add("lunglei");
	   northeast.add("kolasib");
	   northeast.add("lawngtlai");
	   northeast.add("champhai");
	   northeast.add("saiha");
	   northeast.add("serchhip");
	   northeast.add("zunhebotto");
	   northeast.add("dimapur");
	   northeast.add("wokha");
	   northeast.add("phek");
	   northeast.add("mokokchung");
	   northeast.add("kiphire");
	   northeast.add("tuensang");
	   northeast.add("mon");
	   northeast.add("kohima");
	   northeast.add("peren");
	   northeast.add("longleng");
	   northeast.add("south tripura");
	   northeast.add("west tripura");
	   northeast.add("lower dibang valley");
	   northeast.add("east siang");
	   northeast.add("dibang valley");
	   northeast.add("west siang");
	   northeast.add("lohit");
	   northeast.add("papum pare");
	   northeast.add("tawang");
	   northeast.add("west kameng");
	   northeast.add("east kameng");
	   northeast.add("lower subansiri");
	   northeast.add("changlang");
	   northeast.add("tirap");
	   northeast.add("kurung kumey");
	   northeast.add("upper siang");
	   northeast.add("upper subansiri");
	   northeast.add("dhalai");
	   northeast.add("north tripura");
	   northeast.add("west garo hills");
	   northeast.add("east garo hills");
	   northeast.add("jaintia hills");
	   northeast.add("kamrup");
	   northeast.add("east khasi hills");
	   northeast.add("south garo hills");
	   northeast.add("ri bhoi");
	   northeast.add("west khasi hills");
	   northeast.add("goalpara");
	   northeast.add("marigaon");
	      

	   List < String > punjab = new ArrayList < String > ();
	   punjab.add("ludhiana");
	   punjab.add("patiala");
	   punjab.add("bhatinda");
	   punjab.add("hoshiyarpur");
	   punjab.add("hoshiarpur");
	   punjab.add("jalandhar");
	   punjab.add("amritsar");
	   punjab.add("barnala");
	   punjab.add("bathinda");
	   punjab.add("faridkot");
	   punjab.add("firozpur");
	   punjab.add("gurdaspur");
	   punjab.add("ludhiana");
	   punjab.add("kapurthala");
	   punjab.add("mansa");
	   punjab.add("moga");
	   punjab.add("pathankot");
	   punjab.add("chandigarh");
	   punjab.add("panchkula");
	   punjab.add("patiala");
	   punjab.add("sangrur");
	   
	   
	  
	   List < String > odisha = new ArrayList < String > ();
	   odisha.add("ganjam");
	   odisha.add("gajapati");
	   odisha.add("kalahandi");
	   odisha.add("nuapada");
	   odisha.add("koraput");
	   odisha.add("rayagada");
	   odisha.add("nabarangapur");
	   odisha.add("malkangiri");
	   odisha.add("kandhamal");
	   odisha.add("boudh");
	   odisha.add("baleswar");
	   odisha.add("bhadrak");
	   odisha.add("kendujhar");
	   odisha.add("khorda");
	   odisha.add("puri");
	   odisha.add("cuttack");
	   odisha.add("jajapur");
	   odisha.add("kendrapara");
	   odisha.add("jagatsinghapur");
	   odisha.add("mayurbhanj");
	   odisha.add("nayagarh");
	   odisha.add("balangir");
	   odisha.add("sonapur");
	   odisha.add("angul");
	   odisha.add("dhenkanal");
	   odisha.add("sambalpur");
	   odisha.add("bargarh");
	   odisha.add("jharsuguda");
	   odisha.add("debagarh");
	   odisha.add("sundergarh");
	   

	   
	   

	   List < String > tamilnadu = new ArrayList < String > ();
	   tamilnadu.add("vellore");
	   tamilnadu.add("tiruvannamalai");
	   tamilnadu.add("kanchipuram");
	   tamilnadu.add("tiruvallur");
	   tamilnadu.add("pondicherry");
	   tamilnadu.add("villupuram");
	   tamilnadu.add("cuddalore");
	   tamilnadu.add("coimbatore");
	   tamilnadu.add("dharmapuri");
	   tamilnadu.add("salem");
	   tamilnadu.add("erode");
	   tamilnadu.add("karur");
	   tamilnadu.add("namakkal");
	   tamilnadu.add("krishnagiri");
	   tamilnadu.add("nilgiris");
	   tamilnadu.add("dindigul");
	   tamilnadu.add("kanyakumari");
	   tamilnadu.add("sivaganga");
	   tamilnadu.add("ramanathapuram");
	   tamilnadu.add("tuticorin");
	   tamilnadu.add("tirunelveli");
	   tamilnadu.add("madurai");
	   tamilnadu.add("theni");
	   tamilnadu.add("virudhunagar");
	   tamilnadu.add("ariyalur");
	   tamilnadu.add("tiruchirappalli");
	   tamilnadu.add("pudukkottai");
	   tamilnadu.add("tiruvarur");
	   tamilnadu.add("thanjavur");
	   tamilnadu.add("nagapattinam");
	   tamilnadu.add("karaikal");
	   tamilnadu.add("pondicherry");
	   tamilnadu.add("karaikal");
	   tamilnadu.add("perambalur");
	   
	   
	   
	   
	   

	   List < String > westbengal = new ArrayList < String > ();
	   westbengal.add("durgapur");
	   westbengal.add("howrah");
	   westbengal.add("darjeeling");
	   westbengal.add("siliguri");
	   westbengal.add("asansol");
	   westbengal.add("midnapore");
	   westbengal.add("bolpur");
	   westbengal.add("kalyani");
	   westbengal.add("bankura");
	   westbengal.add("howrah");
	   westbengal.add("midnapore");
	   westbengal.add("hooghly");
	   westbengal.add("kharagpur");
	   
	   
	   

	   List < String > gujarat = new ArrayList < String > ();
	   gujarat.add("surat");
	   gujarat.add("ahmedabad");
	   gujarat.add("gandhinagar");
	   gujarat.add("rajkot");
	   gujarat.add("porbandar");
	   gujarat.add("patan");
	   gujarat.add("panchmahal");
	   gujarat.add("navsari");
	   gujarat.add("narmada");
	   gujarat.add("morbi");
	   gujarat.add("mehsana");
	   gujarat.add("mahisagar");
	   gujarat.add("kheda");
	   gujarat.add("kutch");
	   gujarat.add("dang");
	   gujarat.add("dahod");
	   gujarat.add("botad");
	   gujarat.add("bhavnagar");
	   gujarat.add("bharuch");
	   gujarat.add("banaskantha");
	   gujarat.add("aravali");
	   gujarat.add("anand");
	   gujarat.add("amreli");
	   gujarat.add("banaskantha");
	   gujarat.add("mahesana");
	   gujarat.add("surendra nagar");
	   gujarat.add("patan");
	   gujarat.add("sabarkantha");
	   gujarat.add("amreli");
	   gujarat.add("rajkot");
	   gujarat.add("junagadh");
	   gujarat.add("bhavnagar");
	   gujarat.add("jamnagar");
	   gujarat.add("porbandar");
	   gujarat.add("kachchh");
	   gujarat.add("anand");
	   gujarat.add("kheda");
	   gujarat.add("surat");
	   gujarat.add("the dangs");
	   gujarat.add("tapi");
	   gujarat.add("navsari");
	   gujarat.add("vadodara");
	   gujarat.add("bharuch");
	   gujarat.add("panch mahals");
	   gujarat.add("valsad");
	   gujarat.add("diu");
	   gujarat.add("daman");
	     
	   List < String > madhyapradesh = new ArrayList < String > ();
	   madhyapradesh.add("bhopal");
	   madhyapradesh.add("indore");
	   madhyapradesh.add("jabalpur");
	   madhyapradesh.add("ujjain");
	   madhyapradesh.add("gwalior");
	   madhyapradesh.add("ratlam");
	   madhyapradesh.add("neemuch");
	   madhyapradesh.add("chhatarpur");
	   madhyapradesh.add("singrauli");
	   madhyapradesh.add("sidhi");
	   madhyapradesh.add("satna");
	   madhyapradesh.add("rewa");
	   madhyapradesh.add("hoshangabad");
	   madhyapradesh.add("harda");
	   madhyapradesh.add("seoni");
	   madhyapradesh.add("dindori");
	   madhyapradesh.add("mandla");
	   madhyapradesh.add("katni");
	   madhyapradesh.add("chhindwara");
	   madhyapradesh.add("balaghat");
	   madhyapradesh.add("jhabua");
	   madhyapradesh.add("dhar");
	   madhyapradesh.add("burhanpur");
	   madhyapradesh.add("barwani");
	   madhyapradesh.add("alirajpur");
	   madhyapradesh.add("guna");
	   madhyapradesh.add("datia");
	   madhyapradesh.add("shivpuri");
	   madhyapradesh.add("ashoknagar");
	   madhyapradesh.add("sehore");
	   madhyapradesh.add("rajgarh");
	   madhyapradesh.add("kanker");
	   madhyapradesh.add("bastar");
	   madhyapradesh.add("dantewada");
	   madhyapradesh.add("bijapur");
	   madhyapradesh.add("narayanpur");
	   madhyapradesh.add("bilaspur");
	   madhyapradesh.add("janjgir-champa");
	   madhyapradesh.add("korba");
	   madhyapradesh.add("durg");
	   madhyapradesh.add("rajnandgaon");
	   madhyapradesh.add("kawardha");
	   madhyapradesh.add("surguja");
	   madhyapradesh.add("raigarh");
	   madhyapradesh.add("jashpur");
	   madhyapradesh.add("koriya");
	   madhyapradesh.add("raipur");
	   madhyapradesh.add("mahasamund");
	   madhyapradesh.add("dhamtari");
	   madhyapradesh.add("gariaband");
	   
	  
	   List < String > kerala = new ArrayList < String > ();
	   kerala.add("kochi");
	   kerala.add("thiruvananthapuram");
	   kerala.add("thrissur");
	   kerala.add("wayanad");
	   kerala.add("kollam");
	   kerala.add("kannur");
	   kerala.add("alappuzha");
	   
	   List < String > andhrapradesh = new ArrayList < String > ();
	   andhrapradesh.add("hyderabad");
	   andhrapradesh.add("vijayawada");
	   andhrapradesh.add("secunderabad");
	   andhrapradesh.add("vizianagaram");
	   andhrapradesh.add("prakasam");
	   andhrapradesh.add("west godavari");
	   andhrapradesh.add("kurnool");
	   andhrapradesh.add("srikakulam");
	   andhrapradesh.add("ysr");
	   andhrapradesh.add("visakhapatnam");
	   andhrapradesh.add("krishna");
	   andhrapradesh.add("ananthapur");
	   andhrapradesh.add("cuddapah");
	   andhrapradesh.add("chittoor");
	   andhrapradesh.add("kurnool");
	   andhrapradesh.add("prakasam");
	   andhrapradesh.add("west godavari");
	   andhrapradesh.add("krishna");
	   andhrapradesh.add("nellore");
	   andhrapradesh.add("guntur");
	   andhrapradesh.add("east godavari");
	   andhrapradesh.add("visakhapatnam");
	   andhrapradesh.add("vizianagaram");
	   andhrapradesh.add("srikakulam");
	   andhrapradesh.add("adilabad");
	   andhrapradesh.add("warangal");
	   andhrapradesh.add("karim nagar");
	   andhrapradesh.add("mahabub nagar");
	   andhrapradesh.add("k.v.rangareddy");
	   andhrapradesh.add("medak");
	   andhrapradesh.add("nalgonda");
	   andhrapradesh.add("nizamabad");
	   andhrapradesh.add("hyderabad");
	   andhrapradesh.add("khammam");
	   
	   List < String > jammu = new ArrayList < String > ();
	   jammu.add("srinagar");
	   jammu.add("jammu");
	   jammu.add("doda");
	   jammu.add("kishtwar");
	   jammu.add("rajouri");
	   jammu.add("reasi");
	   jammu.add("udhampur");
	   jammu.add("ramban");
	   jammu.add("kathua");
	   jammu.add("samba");
	   jammu.add("poonch");
	   jammu.add("anantnag");
	   jammu.add("kulgam");
	   jammu.add("kargil");
	   jammu.add("leh");
	   jammu.add("kupwara");
	   jammu.add("ganderbal");
	   

	   List < String > assam = new ArrayList < String > ();
	   assam.add("guwahati");
	   assam.add("silchar");
	   assam.add("jorhat");
	   assam.add("tezpur");
	   assam.add("dibrugarh");

	   List < String > uttarpradesheast = new ArrayList < String > ();
	   uttarpradesheast.add("lucknow");
	   uttarpradesheast.add("kanpur");
	   uttarpradesheast.add("varanasi");
	   uttarpradesheast.add("gorakhpur");
	   uttarpradesheast.add("allahabad");
	   uttarpradesheast.add("ajamgarh");
	   uttarpradesheast.add("faizabad");
	   uttarpradesheast.add("moradabad");
	   uttarpradesheast.add("rampur");
	   uttarpradesheast.add("barelly");
	   uttarpradesheast.add("bareilly");
	   uttarpradesheast.add("jhansi");
	   uttarpradesheast.add("banaras");
	   uttarpradesheast.add("mathura");
	   uttarpradesheast.add("barabanki");
	   uttarpradesheast.add("sultanpur");
	   uttarpradesheast.add("saharanpur");
	   uttarpradesheast.add("sitapur");
	   uttarpradesheast.add("lakhimpur");
	   uttarpradesheast.add("pilibhit");
	   uttarpradesheast.add("hardoi");

	   List < String > uttarpradeshwest = new ArrayList < String > ();
	   uttarpradeshwest.add("meerut");
	   uttarpradeshwest.add("bulandshahr");
	   uttarpradeshwest.add("hapur");
	   uttarpradeshwest.add("baghpat");
	   uttarpradeshwest.add("saharanpur");
	   uttarpradeshwest.add("shamli");
	   uttarpradeshwest.add("moradabad");
	   uttarpradeshwest.add("bijnor");
	   uttarpradeshwest.add("rampur");
	   uttarpradeshwest.add("barelly");
	   uttarpradeshwest.add("pilibhit");
	   uttarpradeshwest.add("shahjahanpur");
	   uttarpradeshwest.add("amroha");
	   uttarpradeshwest.add("hathras");
	   uttarpradeshwest.add("kasganj");
	   uttarpradeshwest.add("baghpat");
	   uttarpradeshwest.add("dehradun");
	   uttarpradeshwest.add("champawat");
	   uttarpradeshwest.add("chamoli");
	   uttarpradeshwest.add("bageshwar");
	   uttarpradeshwest.add("almora");
	   uttarpradeshwest.add("nainital");
	   uttarpradeshwest.add("uttarkashi");
	   uttarpradeshwest.add("haridwar");
	   uttarpradeshwest.add("aligarh");
	   uttarpradeshwest.add("mathura");
	  
	   List < String > delhi = new ArrayList < String > ();
	   delhi .add("ghaziabad");
	   delhi .add("noida");
	   delhi .add("faridabad");
	   delhi .add("gurgaon");
	   delhi .add("delhi");

	   map.put("Maharashtra", maharashtra);
	   map.put("Rajasthan", rajasthan);
	   map.put("Karnataka", karnataka);
	   map.put("Bihar", bihar);
	   map.put("Punjab", punjab);
	   map.put("Orissa", odisha);
	   map.put("Tamilnadu", tamilnadu);
	   map.put("WestBengal", westbengal);
	   map.put("Gujarat", gujarat);
	   map.put("Jammu", jammu);
	   map.put("Assam", assam);
	   map.put("Kerala", kerala);
	   map.put("UttarPradeshEast", uttarpradesheast);
	   map.put("AndhraPradesh", andhrapradesh);
	   map.put("MadhyaPradesh", madhyapradesh);
	   map.put("NorthEast", northeast);
	   map.put("HimachalPradesh", himachal);
	   map.put("UttarPradeshWest", uttarpradeshwest);
	   map.put("Haryana", haryana);
	   map.put("Delhi", delhi);
	   attributeName = "location";
	   attributeValue = "";
	   try {
	if (circle.equals("Mumbai")) {
	    priority = 1.0;
	    attributeValue = "Mumbai";
	   }
	else if (circle.equals("Kolkata")) {
	    priority = 1.0;
	    attributeValue = "Kolkata";
	   }
	else  if (circle.equals("Chennai")) {
	    priority = 1.0;
	    attributeValue = "Chennai";
	   }
	else  if (circle.equals("Delhi")) {
	    priority = 1.0;
	    attributeValue = "Delhi";
	   }

	else if (smstxt.contains("sitapura industrial area")
	|| smstxt.contains("haldi ghati gate")
	|| smstxt.contains("sanganer") 
	|| smstxt.contains("tonk phatak") 
	|| smstxt.contains("sawai mansingh stadium") 
	|| smstxt.contains("narayan singh circle")
	|| smstxt.contains("sawai man singh hospital") 
	|| smstxt.contains("panipech")
	|| smstxt.contains("ambabari") 
	|| smstxt.contains("new aatish market")
	|| smstxt.contains("gurjar ki thadi")
	|| smstxt.contains("hawa sadak")
	|| smstxt.contains("chandpole")
	|| smstxt.contains("chhoti chaupar")
	|| smstxt.contains("badi chaupar") && circle.contains("Rajasthan") ){
	  	     priority = 2.03;
			      
	  	   attributeValue="jaipur";
	    	} 
	else if ( smstxt.contains("sushant lok")|| smstxt.contains("dlf cyber city")|| smstxt.contains("dlf phase 2")|| smstxt.contains("dlf phase 3")|| smstxt.contains("sikandarpur")|| smstxt.contains("belvedere Towers")|| smstxt.contains("moulsari Avenue")|| smstxt.contains("iffco chowk")|| smstxt.contains("gurgaon one")|| smstxt.contains("arailas")|| smstxt.contains("hamilton court")|| smstxt.contains("sarhol")|| smstxt.contains("sukhrali")||smstxt.contains("sohna")||smstxt.contains("hero honda chowk")|| smstxt.contains("kanhai")|| smstxt.contains("jharsa")|| smstxt.contains("medicity") )
	{

	attributeValue= "gurgaon";
	priority = 2.03;
	}
	else if ( (smstxt.contains("charminar ")|| smstxt.contains("salar jung museum ")|| smstxt.contains("nizam's museum ")|| smstxt.contains("falaknuma palace ")|| smstxt.contains("laad bazaar ")|| smstxt.contains("madina circle ")|| smstxt.contains("begum bazaar ")|| smstxt.contains("abids ")|| smstxt.contains("sultan bazaar ")|| smstxt.contains("moazzam jahi market  ")|| smstxt.contains("telangana secretariat ")|| smstxt.contains("hyderabad mint ")|| smstxt.contains("telangana legislature ")|| smstxt.contains("nizam club ")|| smstxt.contains("ravindra bharathi ")|| smstxt.contains("tank bund road ")|| smstxt.contains("rani gunj ")|| smstxt.contains("secunderabad railway station  ")|| smstxt.contains("sanjeevaiah park  ")|| smstxt.contains("lumbini park ")|| smstxt.contains("ntr gardens ")|| smstxt.contains("buddha statue tankbund park ")|| smstxt.contains("banjara hills ")|| smstxt.contains("jubilee hills ")|| smstxt.contains("begumpet ")|| smstxt.contains("khairatabad  ")|| smstxt.contains("miyapur  ")|| smstxt.contains("sanathnagar ")|| smstxt.contains("moosapet ")|| smstxt.contains("balanagar ")|| smstxt.contains("patancheru")) && circle.contains("AndhraPradesh")){

	attributeValue= "hyderabad";
	priority = 2.03;
	}
	else if (smstxt.contains("bitten market ")|| smstxt.contains("upper lake ")|| smstxt.contains("shahpura lake ")|| smstxt.contains("lower lake ")|| smstxt.contains("kaliasot dam ")|| smstxt.contains("kerwa dam ")|| smstxt.contains("bhadbhada ")|| smstxt.contains("char imli ")|| smstxt.contains("arera colony ")|| smstxt.contains("db mall ")|| smstxt.contains("lalghati") ) {

	attributeValue= "bhopal";
	priority = 2.03;
	}
	else if (( smstxt.contains("panaji ")|| smstxt.contains("bicholim ")|| smstxt.contains("mapusa ")|| smstxt.contains("anjuna")|| smstxt.contains("arambol")|| smstxt.contains("baga")|| smstxt.contains("bambolim")|| smstxt.contains("calangute")|| smstxt.contains("candolim")|| smstxt.contains("chapora")|| smstxt.contains("dona paula")|| smstxt.contains("miramar")|| smstxt.contains("morjim")|| smstxt.contains("sinquerim")|| smstxt.contains("vagator")|| smstxt.contains("agonda")|| smstxt.contains("benaulim")|| smstxt.contains("bogmalo")|| smstxt.contains("cavelossim")|| smstxt.contains("colva")|| smstxt.contains("majorda")|| smstxt.contains("mobor")|| smstxt.contains("palolem") )) {

	attributeValue= "goa";
	priority = 2.03;
	}
	else if ( smstxt.contains("hiravadi")|| smstxt.contains("naroda st workshop")|| smstxt.contains("chandola lake")|| smstxt.contains("brts workshop")|| smstxt.contains("kashiram textiles")|| smstxt.contains("narol")|| smstxt.contains("isanpur")|| smstxt.contains("ghodasar")|| smstxt.contains("jashodanagar cross roads")|| smstxt.contains("ctm cross roads")|| smstxt.contains("purvdeep society")|| smstxt.contains("bapunagar approach")|| smstxt.contains("lilanagar")|| smstxt.contains("thakkarnagar approach")|| smstxt.contains("hiravadi")|| smstxt.contains("krishna nagar")|| smstxt.contains("ghuma gam")|| smstxt.contains("bopal")|| smstxt.contains("ambli gam")|| smstxt.contains("jayantilal park")|| smstxt.contains("ramdevnagar")|| smstxt.contains("ajit mill")|| smstxt.contains("odhav fire station")|| smstxt.contains("chhotalal ni chali")|| smstxt.contains("morlidhar society")|| smstxt.contains("odhav talav (gam)")|| smstxt.contains("bhuyangdev")|| smstxt.contains("sattadhar")|| smstxt.contains("sola bridge")|| smstxt.contains("zundal circle")|| smstxt.contains("chandkheda (gam)")|| smstxt.contains("visat gandhinagar junction")|| smstxt.contains("motera cross roads")|| smstxt.contains("kankaria telephone exchange") || smstxt.contains("bhairavnath road") || smstxt.contains("kankaria lake")|| smstxt.contains("maninagar")|| smstxt.contains("geening press")|| smstxt.contains("naroda fruit market")|| smstxt.contains("memco cross road")|| smstxt.contains("naroda")|| smstxt.contains("panjrapol cross roads")|| smstxt.contains("gulbai tekra approach")|| smstxt.contains("raikhad cross roads")|| smstxt.contains("astodiya chakla")|| smstxt.contains("astodiya darwaja")|| smstxt.contains("ranip")|| smstxt.contains("khodiyarnagar")|| smstxt.contains("danilimda cross roads")|| smstxt.contains("vaikunthdham mandir")|| smstxt.contains("raipur darwaja")|| smstxt.contains("karnamukteshwar mahadev")|| smstxt.contains("sarangpur darwaja")|| smstxt.contains("kalupur railway station (ahmedabad central)")|| smstxt.contains("sarkari litho press (delhi darwaja)")|| smstxt.contains("gurudwara")|| smstxt.contains("juna vadaj")|| smstxt.contains("ramapir no tekro")|| smstxt.contains("nava vadaj")|| smstxt.contains("jaymangal")|| smstxt.contains("valinath chowk")|| smstxt.contains("memnagar")|| smstxt.contains("shivranjani")|| smstxt.contains("dharnidhar derasar") || smstxt.contains("anjali (vasna)") && circle.contains("Gujarat") ){

	attributeValue= "ahmedabad";
	priority = 2.03;
	}

	else if ( (smstxt.contains("erandawane")|| smstxt.contains("kothrud")|| smstxt.contains("bund garden")|| smstxt.contains("koregaon park")|| smstxt.contains("swargate")|| smstxt.contains("sahakarnagar")|| smstxt.contains("bibvewadi")|| smstxt.contains("gultekdi")|| smstxt.contains("salisbury park")|| smstxt.contains("khadki")|| smstxt.contains("aundh")|| smstxt.contains("ganeshkhind")|| smstxt.contains("paud road")|| smstxt.contains("dattawadi")|| smstxt.contains("yerwada")|| smstxt.contains("wadgaon sheri")|| smstxt.contains("vishrantwadi")|| smstxt.contains("ghorpadi")|| smstxt.contains("fatimanagar")|| smstxt.contains("wanowrie")|| smstxt.contains("hadapsar")|| smstxt.contains("baner")|| smstxt.contains("balewadi")|| smstxt.contains("pashan")|| smstxt.contains("bavdhan")|| smstxt.contains("karve-nagar")|| smstxt.contains("warje")|| smstxt.contains("wadgaon budrukh")|| smstxt.contains("katraj")|| smstxt.contains("khed shivapur")|| smstxt.contains("wanawadi")|| smstxt.contains("nibm")|| smstxt.contains("lullanagar")|| smstxt.contains("kondhwa")|| smstxt.contains("undri")|| 
			smstxt.contains("wagholi")|| smstxt.contains("kharadi")|| smstxt.contains("viman nagar")|| smstxt.contains("mundhwa dhanori")|| 
			smstxt.contains("kalas")|| smstxt.contains("mahalunge")|| smstxt.contains("sus")|| smstxt.contains("bavdhan budrukh")|| 
			smstxt.contains("kirkatwadi")|| smstxt.contains("pisoli")|| smstxt.contains("lohegaon")|| smstxt.contains("kondhwe dhavde")|| 
			smstxt.contains("kopare")|| smstxt.contains("nande")|| smstxt.contains("khadakwasla")|| smstxt.contains("sadesatra nali")||
			smstxt.contains("ambegaon khurd")|| smstxt.contains("holkarwadi")|| smstxt.contains("wadachiwadi")|| smstxt.contains("shiwalewadi")||
			smstxt.contains("phursungi")|| smstxt.contains("yeolewadi pimpri")|| smstxt.contains("chikhli")|| smstxt.contains("kalewadi")|| smstxt.contains("kasarwadi")|| smstxt.contains("phugewadi")|| smstxt.contains("pimple saudagar")|| smstxt.contains("chinchwadgaon")|| smstxt.contains("thergaon")|| 
			smstxt.contains("tathawade")|| smstxt.contains("old sangvi")|| smstxt.contains("wakad")|| smstxt.contains("hinjawadi")||
			smstxt.contains("pimple nilakh")|| smstxt.contains("dudulgaon")|| smstxt.contains("charholi budruk")|| smstxt.contains("akurdi")||
					smstxt.contains("nigdi")|| smstxt.contains("ravet")|| smstxt.contains("talawade")) && circle.contains("Maharashtra")){

	attributeValue= "pune";
	priority = 2.03;
	}

	else if ( (smstxt.contains("baiyappanahalli")|| smstxt.contains("halasuru") || smstxt.contains("cubbon park") || smstxt.contains("vidhana soudha") || smstxt.contains("magadi road") || smstxt.contains("hosahalli") || smstxt.contains("attiguppe") || smstxt.contains("deepanjali nagar") || smstxt.contains("mysore road")|| smstxt.contains("nagasandra") || smstxt.contains("dasarahalli") || smstxt.contains("jalahalli") || smstxt.contains("peenya industry") || smstxt.contains("peenya") || smstxt.contains("yeshwanthpur") || smstxt.contains("rajajinagar") || smstxt.contains("kuvempu road") || smstxt.contains("srirampura") || smstxt.contains("sampige road")|| smstxt.contains("chickpete") || smstxt.contains("lalbagh") || smstxt.contains("jayanagar") || smstxt.contains("banashankari") || smstxt.contains("jayaprakash nagar") || smstxt.contains("putenahalli")) 
			&& circle.contains("")) {        //todo go

	attributeValue= "bangalore";
	priority = 2.03;
	}
	else if (smstxt.contains("debited") && smstxt.contains("cash-atm") && smstxt.contains("bank"))
	   {
		priority = 2.09;
		   int c=0;
		   String sms_sub,q,b,a;
		    c=smstxt.indexOf("cash-atm");
		    
		   
		    sms_sub = smstxt.substring(c,smstxt.length());
		
		  
		   a=  sms_sub.substring(sms_sub.indexOf("/")+1,sms_sub.length());
		   b= a.substring(a.indexOf("/")+1,a.length());
		   q= b.substring(0, b.indexOf("/"));
		   attributeValue=q;
		      
	   }
	else if (fromNumber.contains("BARCHD")  ){
	    priority = 2.11;
		      
	  attributeValue="chandigarh";
		} 

	   else if (smstxt.contains("vehicle")){
		   String a="";
		   Pattern p = Pattern.compile("(([A-Za-z]){2,3}(|-)(?:[0-9]){1,2}(|-)(?:[A-Za-z]){2}(|-)([0-9]){1,4})");
			Matcher m = p.matcher(smstxt);
			while (m.find()) {
				a= m.group();
				
	            break;

			                  }
			attributeValue=a;
			priority = 4.01;
	   }
	   else if (smstxt.contains("pincode") || (smstxt.contains("pin") && smstxt.contains("code")))
	   {
		  
		  String a="";
		   Pattern p = Pattern.compile("(([0-9]){6})");
			Matcher m = p.matcher(smstxt);
			while (m.find()) {
				a= m.group();
				
	            break;

			                  }
			attributeValue=a;
			priority = 5.01;
	   }
	   else if (smstxt.contains("atm id"))
	   {
	   priority = 5.02;
	      int c=0;
	       c=smstxt.indexOf("atm id")-1;
	       String s = smstxt.substring(0,c);
	        String arr[] = s.split(" ");
	        int a=(arr.length);
	        String value=arr[a-1];
	        attributeValue=value;
	       }
	   else if (smstxt.contains("ifsc") && !(smstxt.contains("beneficiary added"))){
	   	   String a="";
	   	   if (smstxt.contains("ifsc")){
	   		      a=smstxt.substring(smstxt.indexOf("ifsc")+5,smstxt.indexOf("ifsc")+17);
	   		      a=a.replace(" ","");
	   		      attributeValue=a;
	   		      priority = 5.03;
	   	   			} 
	      		}
		   
	   else if ((smstxt.contains("arunachal pradesh") || smstxt.contains("meghalaya") ||smstxt.contains("mizoram") ||smstxt.contains("manipur") || smstxt.contains("nagaland") ||smstxt.contains("tripura")) && circle.contains("NorthEast")){
		 
		   priority = 6.01;
		   if (smstxt.contains("arunachal pradesh")){
			      attributeValue= "arunachal pradesh";
			    
		   	} 
		   else if (smstxt.contains("meghalaya")){
			   attributeValue = "meghalaya";
			   
		   }
		   else if (smstxt.contains("mizoram"))
		   {
			   attributeValue = "mizoram";
		   }
		   else if (smstxt.contains("manipur"))
		   {
			   attributeValue = "manipur";
		   }
		   else if (smstxt.contains("nagaland"))
		   {
			   attributeValue = "nagaland";
		   }
		   else if (smstxt.contains("tripura"))
		   {
		   attributeValue = "tripura";
	}
	}
	if (priority >2.01 || priority==0){ 
		   for (Map.Entry < String, List < String >> entry: map.entrySet()) {
			    String keyss = entry.getKey();
			    if (circle.equals(keyss)) {
			     List < String > values = entry.getValue();
			     for (String city: values) {
			      if (smstxt.contains(city) && !smstxt.contains(city + "road")) {
			       priority = 2.01;
			       attributeValue = city;
			       } // to do 
			     }
			   }
			  }  
			  }

	    
	    if (attributeValue.length() > 0){
	    	
	     context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t" +fromNumber + "\t"+Double.toString(priority) + "\t" + usercategory));
	    }
	    } catch (InterruptedException e) {
	    
	    e.printStackTrace();
	   }
	   return  String.valueOf(attributeValue);	
	  }
	 
	  public  String username(String smstxt, String smsId, String phoneNo,String userid, String time, Context context,String fromNumber,String operator,String circle, String usercategory) throws IOException {


		  attributeName = "name";
		    attributeValue = "";
		    priority=1.01;
		    String str1,name;
		    String sms1[] = new String[100];
		    int a=100,b=100;
		    try {
		    	if (smstxt.startsWith("dear mr.") ){
			    	
			    	 sms1 = smstxt.split(" ");
			    	 name  =sms1[2];
			    	 attributeValue = name;
			        }
		    	else if (smstxt.startsWith("dear miss."))
		    	{
		    		 sms1 = smstxt.split(" ");
			    	 name  =sms1[2];
			    	 attributeValue = name;
		    		
		    	}
			    else if (smstxt.startsWith("dear mrs.") ){
			    	
			    	 sms1 = smstxt.split(" ");
			    	 name  =sms1[2];
			    	 attributeValue = name;
			        }
			    else if (smstxt.startsWith("hello mr.") ){
			    	
			    	 sms1 = smstxt.split(" ");
			    	 name  =sms1[2];
			    	 attributeValue = name;
			        }
			    else if (smstxt.startsWith("hello mrs.") ){
			    	
			    	 sms1 = smstxt.split(" ");
			    	name  =sms1[2];
			    	 attributeValue = name;
			        }
			    else if (smstxt.startsWith("hi mr.") ){
			    	
			    	 sms1 = smstxt.split(" ");
			    	 name  =sms1[2];
			    	 attributeValue = name;
			        }
			    else if (smstxt.startsWith("hi mrs.") ){
			    	
			    	 sms1 = smstxt.split(" ");
			    	 name  =sms1[2];
			    	 attributeValue = name;
			        }
		    	else if ((smstxt.startsWith("dear ")) && (!smstxt.contains("dear customer") && !smstxt.contains("dear investor") && !smstxt.contains("dear counseling") && !smstxt.contains("dear valued") && !smstxt.contains("dear club") && !smstxt.contains("dear guest") && !smstxt.contains("dear incumbent") && !smstxt.contains("dear aviva") && !smstxt.contains("dear sir/mam") && !smstxt.contains("dear axis") && !smstxt.contains("dear partner") && !smstxt.contains("dear iterm") && !smstxt.contains("dear cbm/bm") && !smstxt.contains("dear msdian") && !smstxt.contains("dear csp") && !smstxt.contains("dear policyholder") && !smstxt.contains("dear channel") && !smstxt.contains("dear d2h") && !smstxt.contains("dear lvb") && !smstxt.contains("dear administrator") && !smstxt.contains("dear associate") && !smstxt.contains("dear parent") && !smstxt.contains("dear sir/madam") && !smstxt.contains("dear student") && !smstxt.contains("dear met") && !smstxt.contains("dear tmf") && !smstxt.contains("dear your") && !smstxt.contains("dear abcd") && !smstxt.contains("dear aegon") && !smstxt.contains("dear aspirant") && !smstxt.contains("dear asm") && !smstxt.contains("dear auditor") && !smstxt.contains("dear auro") && !smstxt.contains("dear bfl") && !smstxt.contains("dear bh") && !smstxt.contains("dear bussiness") && !smstxt.contains("dear candidates") && !smstxt.contains("dear cl84") && !smstxt.contains("dear crew") && !smstxt.contains("dear cust") && !smstxt.contains("dear dealor") && !smstxt.contains("dear deposit") && !smstxt.contains("dear dolphin") && !smstxt.contains("dear donor") && !smstxt.contains("dear dp") && !smstxt.contains("dear dotcabs") && !smstxt.contains("dear driver") && !smstxt.contains("dear donor") 
		            &&!smstxt.contains("dear emp") && !smstxt.contains("dear fac") && !smstxt.contains("dear fino") && !smstxt.contains("dear forum") && !smstxt.contains("dear friend") && !smstxt.contains("dear gamebuddy") && !smstxt.contains("dear gas") && !smstxt.contains("dear gsc") && !smstxt.contains("dear gold") && !smstxt.contains("dear guardian") && !smstxt.contains("dear health") && !smstxt.contains("dear host") && !smstxt.contains("dear indigo") && !smstxt.contains("dear instructor") && !smstxt.contains("dear jalan") && !smstxt.contains("dear key") && !smstxt.contains("dear kids")  && !smstxt.contains("dear learner") && !smstxt.contains("dear fac") && !smstxt.contains("dear maben") && !smstxt.contains("dear member") && !smstxt.contains("dear passenger") && !smstxt.contains("dear pensioner") && !smstxt.contains("dear phama") && !smstxt.contains("dear prerana") && !smstxt.contains("dear principal") && !smstxt.contains("dear r.m") && !smstxt.contains("dear relationship") && !smstxt.contains("dear resident") && !smstxt.contains("dear retailer") && !smstxt.contains("dear rhs") && !smstxt.contains("dear rh") && !smstxt.contains("dear rm") && !smstxt.contains("dear rs name") && !smstxt.contains("dear rupantaran") && !smstxt.contains("dear seller") && !smstxt.contains("dear sir / madam") && !smstxt.contains("dear sm/oh") && !smstxt.contains("dear spice") && !smstxt.contains("dear so,") && !smstxt.contains("dear sp,") && !smstxt.contains("dear sri sai") && !smstxt.contains("dear staff") && !smstxt.contains("dear stockholding") && !smstxt.contains("dear subscriber") && !smstxt.contains("dear surveyor") && !smstxt.contains("dear teacher") && !smstxt.contains("dear team") && !smstxt.contains("dear tech") && !smstxt.contains("dear tmf") && !smstxt.contains("dear test") && !smstxt.contains("dear tpddl") && !smstxt.contains("dear tractor") && !smstxt.contains("dear trainer") && !smstxt.contains("dear trustee") && !smstxt.contains("dear ubiuser") 
		            && !smstxt.contains("dear ucoites") && !smstxt.contains("dear user") && !smstxt.contains("dear vaish") 
		            && !smstxt.contains("dear valuefirst") && !smstxt.contains("dear vb") && !smstxt.contains("dear vendor") 
		            && !smstxt.contains("dear visitor")  && !smstxt.contains("dear viproite") && !smstxt.contains("") && !smstxt.contains("dear com.dob")
		            && !smstxt.contains("dear xxx") && !smstxt.contains("dear xyz") && !smstxt.contains("dear zh") && !smstxt.contains("dear respected")
		            && !smstxt.contains("dear member") && !smstxt.contains("dear privilege") && !smstxt.contains("dear patient") && !smstxt.contains("dear prof") && !smstxt.contains("dear rbm") 
		            && !smstxt.contains("dear reader") && !smstxt.contains("dear saving") && !smstxt.contains("dear travel") && !smstxt.contains("dear trustline") && !smstxt.contains("dear vp") && !smstxt.contains("dear consumer") && !smstxt.contains("dear pnm") && !smstxt.contains("dear distributor") && !smstxt.contains("dear colleague") && !smstxt.contains("dear <<xyz>>")  && !smstxt.contains("dear <>") 
		            && !smstxt.contains("dear __") && !smstxt.contains("dear abc") && !smstxt.contains("dear admin") && !smstxt.contains("dear advisor") && !smstxt.contains("dear adsf") && !smstxt.contains("dear agent") && !smstxt.contains("dear agr74") && !smstxt.contains("dear and7") && !smstxt.contains("dear and8") && !smstxt.contains("dear applicant") && !smstxt.contains("dear arli") && !smstxt.contains("dear bsc") && !smstxt.contains("dear bdm") 
		            && !smstxt.contains("dear beneficiary") && !smstxt.contains("dear bm") & !smstxt.contains("dear job") && !smstxt.contains("dear miss.")
		            && !smstxt.contains("dear branch") && !smstxt.contains("dear broker") && !smstxt.contains("dear bsm")
		            && !smstxt.contains("dear buisness") && !smstxt.contains("dear cabin") && !smstxt.contains("dear we")
		            && !smstxt.contains("dear call") && !smstxt.contains("dear sir/mam") && !smstxt.contains("dear sir") && !smstxt.contains("dear mam") && !smstxt.contains("dear madam") && !smstxt.contains("dear client") && !smstxt.contains("dear clients") && !smstxt.contains("dear mr.") && !smstxt.contains("dear mrs.")
		            && !smstxt.contains("dear customer,")&& !smstxt.contains("dear card") && !smstxt.contains("dear insured")
		            && !smstxt.contains("dear cavins") && !smstxt.contains("dear manager") && !smstxt.contains("dear oxigen") && !smstxt.contains("dear shopper") && !smstxt.contains("dear mba") && !smstxt.contains("dear premier") && !smstxt.contains("dear aspirant") && !smstxt.contains("dear paytm") && !smstxt.contains("dear customer") && !smstxt.contains("dear candidate") && !smstxt.contains("dear indane"))) {
		    		str1 =smstxt.substring(smstxt.indexOf(" ")+1, smstxt.length());
		    		             if (str1.contains(","))
		    		             {
		                           a=str1.indexOf(",");
		                                                        
		                         }
		                         b= str1.indexOf(" ");
		                         if (a<b)             {
		                        	                  attributeValue =str1.substring(0,  str1.indexOf(","));
		                        	                 
		                      		                 
		                                               }
		                         else                 {
		                        	                   attributeValue =str1.substring(0,  str1.indexOf(" "));
		                        	             
		                       		                 
		                                               }
		    	
		                        
		    	
		    	
		                                                                                      }
		    	else if ((smstxt.startsWith("hello ")) && (!smstxt.contains("hello loan") && !smstxt.contains("hello emp") && !smstxt.contains("hello fac") && !smstxt.contains("hello fino") && !smstxt.contains("hello forum") && !smstxt.contains("hello friend") && !smstxt.contains("hello gamebuddy") && !smstxt.contains("hello gas") && !smstxt.contains("hello gsc") && !smstxt.contains("hello gold") && !smstxt.contains("hello guardian") && !smstxt.contains("hello health") && !smstxt.contains("hello host") && !smstxt.contains("hello indigo") && !smstxt.contains("hello instructor") && !smstxt.contains("hello jalan") && !smstxt.contains("hello key") && !smstxt.contains("hello kids") && !smstxt.contains("hello la") && !smstxt.contains("hello learner") && !smstxt.contains("hello fac") && !smstxt.contains("hello maben") && !smstxt.contains("hello member") && !smstxt.contains("hello passenger") && !smstxt.contains("hello pensioner") && !smstxt.contains("hello phama") && !smstxt.contains("hello prerana") && !smstxt.contains("hello principal") && !smstxt.contains("hello r.m") && !smstxt.contains("hello relationshellop") && !smstxt.contains("hello resident") && !smstxt.contains("hello retailer") && !smstxt.contains("hello rhs") && !smstxt.contains("hello rh") && !smstxt.contains("hello rm") && !smstxt.contains("hello rs name") && !smstxt.contains("hello rupantaran") && !smstxt.contains("hello seller") && !smstxt.contains("hello sir / madam") && !smstxt.contains("hello sm/oh") && !smstxt.contains("hello spice") && !smstxt.contains("hello so,") && !smstxt.contains("hello sp,") && !smstxt.contains("hello sri sai") && !smstxt.contains("hello staff") && !smstxt.contains("hello stockholding") && !smstxt.contains("hello subscriber") && !smstxt.contains("hello surveyor") && !smstxt.contains("hello teacher") && !smstxt.contains("hello team") && !smstxt.contains("hello tech") && !smstxt.contains("hello tmf") && !smstxt.contains("hello test") && !smstxt.contains("hello tpddl") && !smstxt.contains("hello tractor") && !smstxt.contains("hello trainer") && !smstxt.contains("hello trustee") && !smstxt.contains("hello ubiuser") 
			            && !smstxt.contains("hello ucoites") && !smstxt.contains("hello user") && !smstxt.contains("hello vaish") 
			            && !smstxt.contains("hello valuefirst") && !smstxt.contains("hello vb") && !smstxt.contains("hello vendor") 
			            && !smstxt.contains("hello visitor")  && !smstxt.contains("hello viproite") && !smstxt.contains("hello respected") && !smstxt.contains("hello com.dob")
			            && !smstxt.contains("hello xxx") && !smstxt.contains("hello xyz") && !smstxt.contains("hello zh") 
			            && !smstxt.contains("hello member") && !smstxt.contains("hello privilege") && !smstxt.contains("hello patient") && !smstxt.contains("hello prof") && !smstxt.contains("hello rbm") 
			            && !smstxt.contains("hello reader") && !smstxt.contains("hello saving") && !smstxt.contains("hello travel") && !smstxt.contains("hello trustline") && !smstxt.contains("hello vp") && !smstxt.contains("hello consumer") && !smstxt.contains("hello pnm") && !smstxt.contains("hello distributor") && !smstxt.contains("hello colleague") && !smstxt.contains("hello <<xyz>>")  && !smstxt.contains("hello <>") 
			            && !smstxt.contains("hello __") && !smstxt.contains("hello abc") && !smstxt.contains("hello admin") && !smstxt.contains("hello advisor") && !smstxt.contains("hello adsf") && !smstxt.contains("hello agent") && !smstxt.contains("hello agr74") && !smstxt.contains("hello and7") && !smstxt.contains("hello and8") && !smstxt.contains("hello applicant") && !smstxt.contains("hello arli") && !smstxt.contains("hello bsc") && !smstxt.contains("hello bdm") 
			            && !smstxt.contains("hello beneficiary") && !smstxt.contains("hello bm")  & !smstxt.contains("hello job")
			            && !smstxt.contains("hello branch") && !smstxt.contains("hello broker") && !smstxt.contains("hello bsm") && !smstxt.contains("hello we")
			            && !smstxt.contains("hello buisness") && !smstxt.contains("hello cabin") && !smstxt.contains("hello miss.")
			            && !smstxt.contains("hello call") && !smstxt.contains("hello sir/mam") && !smstxt.contains("hello sir") && !smstxt.contains("hello mam") && !smstxt.contains("hello madam") && !smstxt.contains("hello client") && !smstxt.contains("hello clients") && !smstxt.contains("hello mr.") && !smstxt.contains("hello mrs.")
			            && !smstxt.contains("hello customer,")&& !smstxt.contains("hello card") && !smstxt.contains("hello oxigen")
			            && !smstxt.contains("hello cavins") && !smstxt.contains("hello manager") && !smstxt.contains("hello shopper")
		    			&& !smstxt.contains("hello .y") && !smstxt.contains("hello thanks") && !smstxt.contains("hello ! y") && !smstxt.contains("hello hfrp") && !smstxt.contains("hello tssss") && !smstxt.contains("hello thellos") && !smstxt.contains("hello there") && !smstxt.contains("hello test") && !smstxt.contains("hello the") && !smstxt.contains("hello (name)") && !smstxt.contains("hello -wel") && !smstxt.contains("hello - wel") && !smstxt.contains("hello - your") && !smstxt.contains("hello . Your") && !smstxt.contains("hello manager") && !smstxt.contains("hello shopper") && !smstxt.contains("hello mba") && !smstxt.contains("hello premier") && !smstxt.contains("hello aspirant") && !smstxt.contains("hello paytm") && !smstxt.contains("hello customer") && !smstxt.contains("hello candidate") 
		    			&& !smstxt.contains("hello indane"))) {
		    		str1 =smstxt.substring(smstxt.indexOf(" ")+1, smstxt.length());   
			    	
			    		             if (str1.contains(",")){
			                                                   a=str1.indexOf(",");
			                                                  }
			                         b= str1.indexOf(" ");
			                         if (a<b)             {
			                        	                  attributeValue =str1.substring(0,  str1.indexOf(","));
			                        	             
				                      		                
			                                               }
			                         else                 {
			                        	                   attributeValue =str1.substring(0,  str1.indexOf(" "));
				                      		                
			                                               }
		    	}
		    	else if ((smstxt.startsWith("hi ")) && (!smstxt.contains("hi loan") && !smstxt.contains("hi emp") && !smstxt.contains("hi fac") && !smstxt.contains("hi fino") && !smstxt.contains("hi forum") && !smstxt.contains("hi friend") && !smstxt.contains("hi gamebuddy") && !smstxt.contains("hi gas") && !smstxt.contains("hi gsc") && !smstxt.contains("hi gold") && !smstxt.contains("hi guardian") && !smstxt.contains("hi health") && !smstxt.contains("hi host") && !smstxt.contains("hi indigo") && !smstxt.contains("hi instructor") && !smstxt.contains("hi jalan") && !smstxt.contains("hi key") && !smstxt.contains("hi kids") && !smstxt.contains("hi la") && !smstxt.contains("hi learner") && !smstxt.contains("hi fac") && !smstxt.contains("hi maben") && !smstxt.contains("hi member") && !smstxt.contains("hi passenger") && !smstxt.contains("hi pensioner") && !smstxt.contains("hi phama") && !smstxt.contains("hi prerana") && !smstxt.contains("hi principal") && !smstxt.contains("hi r.m") && !smstxt.contains("hi relationship") && !smstxt.contains("hi resident") && !smstxt.contains("hi retailer") && !smstxt.contains("hi rhs") && !smstxt.contains("hi rh") && !smstxt.contains("hi rm") && !smstxt.contains("hi rs name") && !smstxt.contains("hi rupantaran") && !smstxt.contains("hi seller") && !smstxt.contains("hi sir / madam") && !smstxt.contains("hi sm/oh") && !smstxt.contains("hi spice") && !smstxt.contains("hi so,") && !smstxt.contains("hi sp,") && !smstxt.contains("hi sri sai") && !smstxt.contains("hi staff") && !smstxt.contains("hi stockholding") && !smstxt.contains("hi subscriber") && !smstxt.contains("hi surveyor") && !smstxt.contains("hi teacher") && !smstxt.contains("hi team") && !smstxt.contains("hi tech") && !smstxt.contains("hi tmf") && !smstxt.contains("hi test") && !smstxt.contains("hi tpddl") && !smstxt.contains("hi tractor") && !smstxt.contains("hi trainer") && !smstxt.contains("hi trustee") && !smstxt.contains("hi ubiuser") 
			            && !smstxt.contains("hi ucoites") && !smstxt.contains("hi user") && !smstxt.contains("hi vaish") 
			            && !smstxt.contains("hi valuefirst") && !smstxt.contains("hi vb") && !smstxt.contains("hi vendor") 
			            && !smstxt.contains("hi visitor")  && !smstxt.contains("hi viproite") && !smstxt.contains("hi respected") && !smstxt.contains("hi com.dob")
			            && !smstxt.contains("hi xxx") && !smstxt.contains("hi xyz") && !smstxt.contains("hi zh") 
			            && !smstxt.contains("hi member") && !smstxt.contains("hi privilege") && !smstxt.contains("hi patient") && !smstxt.contains("hi prof") && !smstxt.contains("hi rbm") 
			            && !smstxt.contains("hi reader") && !smstxt.contains("hi saving") && !smstxt.contains("hi travel") && !smstxt.contains("hi trustline") && !smstxt.contains("hi vp") && !smstxt.contains("hi consumer") && !smstxt.contains("hi pnm") && !smstxt.contains("hi distributor") && !smstxt.contains("hi colleague") && !smstxt.contains("hi <<xyz>>")  && !smstxt.contains("hi <>") 
			            && !smstxt.contains("hi __") && !smstxt.contains("hi abc") && !smstxt.contains("hi admin") && !smstxt.contains("hi advisor") && !smstxt.contains("hi adsf") && !smstxt.contains("hi agent") && !smstxt.contains("hi agr74") && !smstxt.contains("hi and7") && !smstxt.contains("hi and8") && !smstxt.contains("hi applicant") && !smstxt.contains("hi arli") && !smstxt.contains("hi bsc") && !smstxt.contains("hi bdm") 
			            && !smstxt.contains("hi beneficiary") && !smstxt.contains("hi bm")  & !smstxt.contains("hi job") && !smstxt.contains("hi we")
			            && !smstxt.contains("hi branch") && !smstxt.contains("hi broker") && !smstxt.contains("hi bsm")
			            && !smstxt.contains("hi buisness") && !smstxt.contains("hi cabin") && !smstxt.contains("hi miss.")
			            && !smstxt.contains("hi call") && !smstxt.contains("hi sir/mam") && !smstxt.contains("hi sir") && !smstxt.contains("hi mam") && !smstxt.contains("hi madam") && !smstxt.contains("hi client") && !smstxt.contains("hi clients") && !smstxt.contains("hi mr.") && !smstxt.contains("hi mrs.")
			            && !smstxt.contains("hi customer,")&& !smstxt.contains("hi card") && !smstxt.contains("hi shopper")
			            && !smstxt.contains("hi cavins") && !smstxt.contains("hi manager") && !smstxt.contains("hi oxigen")
		    			&& !smstxt.contains("hi .y") && !smstxt.contains("hi thanks") && !smstxt.contains("hi ! y") && !smstxt.contains("hi hfrp") && !smstxt.contains("hi tssss") && !smstxt.contains("hi this") && !smstxt.contains("hi there") && !smstxt.contains("hi test") && !smstxt.contains("hi the") && !smstxt.contains("hi (name)") && !smstxt.contains("hi -wel") && !smstxt.contains("hi - wel") && !smstxt.contains("hi - your") && !smstxt.contains("hi . Your") && !smstxt.contains("hi manager") && !smstxt.contains("hi shopper") && !smstxt.contains("hi mba") && !smstxt.contains("hi premier") && !smstxt.contains("hi aspirant") && !smstxt.contains("hi paytm") && !smstxt.contains("hi customer") 
		    			&& !smstxt.contains("hi candidate") && !smstxt.contains("hi indane"))) {
		    		str1 =smstxt.substring(smstxt.indexOf(" ")+1, smstxt.length());   
			    	
			    		             if (str1.contains(",")){
			                                                   a=str1.indexOf(",");
			                                                  }
			                         b= str1.indexOf(" ");
			                         if (a<b)             {
			                        	                  attributeValue =str1.substring(0,  str1.indexOf(","));
			    
			                                               }
			                         else                 {
			                        	                   attributeValue =str1.substring(0,  str1.indexOf(" "));
			  
			                                               }
		    											}
		    		
		    	
		    	 if (attributeValue.length() > 0){
		    	     context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t" +fromNumber + "\t"+Double.toString(priority) + "\t" + usercategory));
		    	    }
		    } catch (Exception e) {
		     e.printStackTrace();
		    }
			return  String.valueOf(attributeValue);
		   }
	  
	  public String premium_date(String smstxt, String smsId, String phoneNo,String userid, String time, Context context, String operator,String circle,String usercategory,String fromNumber) throws IOException {
		   attributeName = "premium_date";
		   
		   String s="";
	           priority = 1.01;
		   attributeValue="";
		   if (smstxt.contains("today is your due date for premium payment ") && smstxt.contains("ICICIPru policy ")){
			   attributeValue=time.substring(0,10);;
			         
			                            }
		else if (smstxt.contains("fund value for policy") && smstxt.contains(" due date for your policy ")){
			   	 s=smstxt.substring(smstxt.indexOf("policy is")+10,smstxt.indexOf("policy is")+18);  // to do 
			   	//s= s.replaceAll("[^\\d.]", "");
			   	attributeValue=s;          
			          }

		   try {
			   
			   if (attributeValue.length() > 0){
				     context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t" +fromNumber + "\t"+Double.toString(priority) + "\t" + usercategory));
				    }	
		   } catch (InterruptedException e) {
		    
		    e.printStackTrace();
		   }
		return  String.valueOf(attributeValue);

		  }



	  public String emailid(String smstxt, String smsId, String phoneNo,String userid, String time, Context context, String operator,String circle,String usercategory,String fromNumber) throws IOException {
	   attributeName = "emailid";
	   attributeValue="";
	   priority = 1.01;
	   
	   if (smstxt.contains("@gmail")  || smstxt.contains("@yahoo") || smstxt.contains("@hotmail") || smstxt.contains("@hotmail") ||
			smstxt.contains("@outlook") || smstxt.contains("@rediff") || smstxt.contains("@mail") || smstxt.contains("@aol") ||
			smstxt.contains("@hushmail") || smstxt.contains("@zohomail")){

		   int idx1 = smstxt.indexOf("@");
		   int firstSpace = smstxt.lastIndexOf(" ", idx1);
		   int lastSpace = smstxt.indexOf(" ", idx1);
		   attributeValue= smstxt.substring(firstSpace+1,lastSpace);

	}  

	   try {
		   
		   if (attributeValue.length() > 0){
			     context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t" +fromNumber + "\t"+Double.toString(priority) + "\t" + usercategory));
			    }
	   } catch (InterruptedException e) {
	    
	    e.printStackTrace();
	   }
	return  String.valueOf(attributeValue);

	  }

	  
	  public String credit_card(String smstxt, String smsId, String phoneNo,String userid, String time, Context context, String operator,String circle,String usercategory ,String fromNumber) throws IOException {
	   attributeName = "credit_card";
	   attributeValue="";
	   if ((smstxt.contains("credit card") || smstxt.contains("creditcard")) && ( smstxt.contains(" rs") || smstxt.contains(" inr") || smstxt.contains("transaction")) ) {
	    priority = 1.01;
	    attributeValue = "Yes";
	   } 
	   else if ((smstxt.contains("cc limit") || smstxt.contains("credit card limit")) && (smstxt.contains("changed") || smstxt.contains("increased")) && (smstxt.contains(" rs") || smstxt.contains(" inr")))
	   {
		    priority = 1.02;
		    attributeValue = "Yes";
	   }
	   else if ((smstxt.contains("available") || smstxt.contains(" avl")) && (smstxt.contains("credit card") || smstxt.contains(" card")))
	   {
		    priority = 1.03;
		    attributeValue = "Yes";
	   }
	   else if (smstxt.contains("card") && (smstxt.contains("payment") || smstxt.contains("increased") || smstxt.contains("receive") || smstxt.contains("changed") || smstxt.contains("blocked") || smstxt.contains("resolved") || smstxt.contains("transaction") || smstxt.contains(" txns"))) 
	   {
	    priority = 2.01;
	    attributeValue = "Yes";
	   }
	   try {
		   
	    if (attributeValue.length() > 0) {
	         context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t"  +fromNumber + "\t" + Double.toString(priority) + "\t" + usercategory));
	         context.write(new Text(smsId+"-"+"saving_account"+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "saving_account" + "\t" + "Yes" + "\t" +circle+ "\t" + operator  +"\t"  +fromNumber + "\t" + "2"+ "\t" + usercategory));
	         context.write(new Text(smsId+"-"+"net_banking"+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "net_banking" + "\t" + "Yes" + "\t" +circle+ "\t" + operator  +"\t"  +fromNumber + "\t" + "2"+ "\t" + usercategory));
	    
	    }
	   } catch (InterruptedException e) {
	    
	    e.printStackTrace();
	   }
	return  String.valueOf(attributeValue);

	  }
	  public String premium_sum(String smstxt, String smsId, String phoneNo,String userid, String time, Context context,String operator,String circle,String fromNumber, String usercategory) throws IOException {
		   attributeName = "premium_sum";
		   attributeValue="";
		   priority = 1.01;
		   String s="";
		   if (smstxt.contains("premium") && smstxt.contains("insurance") &&  (smstxt.contains(" inr") ||smstxt.contains(" rs") )){
		        if (smstxt.contains(" rs")){
		 
		                       s = smstxt.substring(smstxt.indexOf(" rs")+4,smstxt.indexOf(" rs")+12);
		                     
		                       s = s.replaceAll("[^0-9]","");
		                       attributeValue=s;
		                   
		                                 } 
		        else  if (smstxt.contains(" inr")){
		
		                       s = smstxt.substring(smstxt.indexOf(" inr")+4,smstxt.indexOf(" inr")+12 );
		                       
		                       s = s.replaceAll("[^0-9]","");
		                       attributeValue=s;
		                       
		                                 } 
		         
		}

		   try {
			   
			   if (attributeValue.length() > 0){
				     context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t" +fromNumber + "\t"+Double.toString(priority) + "\t" + usercategory));
				    }
		   } catch (InterruptedException e) {
		    
		    e.printStackTrace();
		   }
		return  String.valueOf(attributeValue);

		  }



	  public String car_loan(String smstxt, String smsId, String phoneNo,String userid, String time, String fromNumber, Context context,String operator,String circle, String usercategory) throws IOException {

	   attributeName = "car_loan";
	   attributeValue = "";
	   String t=time;
	   
	   if ((smstxt.contains("car loan") || (smstxt.contains("car") && smstxt.contains("loan"))) && !(smstxt.contains("thank you") && smstxt.contains("clarifications")))
	   {
	    priority = 1.01;
	    attributeValue = "Yes";
	   }

	   else if ( smstxt.contains("auto") && (smstxt.contains(" rs") || smstxt.contains(" inr") ))
	   {
	    priority = 1.02;
	    if (fromNumber.contains("Maruti") || fromNumber.contains("Tata motors") || fromNumber.contains("BFL") || fromNumber.contains("CorpBk") || fromNumber.contains("ECORNT")|| fromNumber.contains("GFSLTD") || fromNumber.contains("ICICIB") || fromNumber.contains("KVBANK") || fromNumber.contains("NKGSB") || fromNumber.contains("AIPNBO") || fromNumber.contains("JAITLY") || fromNumber.contains("RKNNDA"))
	    {
	    attributeValue = "Yes";
	   }// to do
	   }

	   else if ((smstxt.contains("loan")) && (smstxt.contains(" rs") || smstxt.contains(" inr")))
	    {
	        priority = 2.01;
	        if (fromNumber.contains("Maruti") || fromNumber.contains("Tata motors") || fromNumber.contains("BFL") || fromNumber.contains("CorpBk") || fromNumber.contains("ECORNT")|| fromNumber.contains("GFSLTD") || fromNumber.contains("ICICIB") || fromNumber.contains("KVBANK") || fromNumber.contains("NKGSB") || fromNumber.contains("AIPNBO") || fromNumber.contains("JAITLY") || fromNumber.contains("RKNNDA"))
	        {
	        attributeValue = "Yes";
	        } // to do 
	    }
	    	
	    try {
	    	
	     if (attributeValue.length() > 0) {
	      context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid +"\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t" +fromNumber + "\t" + Double.toString(priority) +"\t" + usercategory));
	      context.write(new Text(smsId+"-"+"car_insurance"+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo  +"\t"+ userid +"\t" + t + "\t" + "car_insurance" + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t"  +fromNumber + "\t" + Double.toString(priority) +"\t" + usercategory));
	      context.write(new Text(smsId+"-"+"saving_account"+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo  + "\t"+ userid+"\t" + t + "\t" + "saving_account" + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t"  +fromNumber + "\t" + Double.toString(priority) +"\t" + usercategory));
	      context.write(new Text(smsId+"-"+"net_banking"+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+"\t" + t + "\t" + "net_banking" + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t"   +fromNumber + "\t"+ Double.toString(priority) +"\t" + usercategory));
	     }
	    } catch (InterruptedException e) {
	     e.printStackTrace();
	    }
		return  String.valueOf(attributeValue);
	   }

	  public String car_emiduedate(String smstxt, String smsId, String phoneNo,String userid, String time, String fromNumber, Context context,String operator,String circle, String usercategory) throws IOException {

		   attributeName = "car_emiduedate";
		   attributeValue = "";
		   priority=1.01;
		   String t=time;
		   if (smstxt.contains(" emi") && smstxt.contains(" car") && (smstxt.contains("due on") || smstxt.contains(" due")))
		   {
	  		 String a="";
	  		 String str1;
	  		 
	                    if (smstxt.contains("due on ")){     
				    	    str1 = smstxt.substring(smstxt.indexOf("due on "), smstxt.indexOf("due on ") + 25);
				    	   a= str1.replaceAll("[^\\d-/]", "");

				    	}
				    	else if (smstxt.contains(" due")){     
				    	    str1 = smstxt.substring(smstxt.indexOf("due "), smstxt.indexOf("due ") + 25);
				    	   a= str1.replaceAll("[^\\d-/]", "");

				    	}
			              attributeValue = a;
			     
			                                                                              }

		    try {
		    	
		     if (attributeValue.length() > 0) {
		      context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+"\t"+ t + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t" +fromNumber + "\t" + Double.toString(priority) +"\t" + usercategory));
		      }
		    } catch (InterruptedException e) {
		     e.printStackTrace();
		    }
			return  String.valueOf(attributeValue);
		   }
	  
	  public String home_emiduedate(String smstxt, String smsId, String phoneNo,String userid, String time, String fromNumber, Context context,String operator,String circle, String usercategory) throws IOException {

		   attributeName = "home_emiduedate";
		   attributeValue = "";
		   priority=1.01;
		   String t=time;
		   if ((smstxt.contains(" emi")) && (smstxt.contains(" home") && (smstxt.contains("due on") || smstxt.contains("due date")) )) {
	 		 String a="";
	 		 String str1;
	 		 
	                   if (smstxt.contains("due on ")){     
				    	    str1 = smstxt.substring(smstxt.indexOf("due on "), smstxt.indexOf("due on ") + 25);
				    	   a= str1.replaceAll("[^\\d-/]", "");
				    	}
				    	else if (smstxt.contains(" due")){     
				    	    str1 = smstxt.substring(smstxt.indexOf("due "), smstxt.indexOf("due ") + 25);
				    	   a= str1.replaceAll("[^\\d-/]", "");
				    	}
			              attributeValue = a;
			     
			                                                                              }

		    try {
		    	
		     if (attributeValue.length() > 0) {
		      context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo  +"\t"+ userid+"\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t" +fromNumber + "\t" + Double.toString(priority) +"\t" +usercategory));
		      }
		    } catch (InterruptedException e) {
		     e.printStackTrace();
		    }
			return  String.valueOf(attributeValue);
		   }
	  
	  public String lifeinsurance_emiduedate(String smstxt, String smsId, String phoneNo,String userid, String time, String fromNumber, Context context,String operator,String circle, String usercategory) throws IOException {

		   attributeName = "lifeinsurance_emiduedate";
		   attributeValue = "";
		   String t=time;
		   String a="";
		   String str1;
		   if ((smstxt.contains(" emi")) && (smstxt.contains(" life") && (smstxt.contains("due on") || smstxt.contains("due date")) )) {
		 		priority = 1.01;
		 		 
		                   if (smstxt.contains("due on ")){     
					    	    str1 = smstxt.substring(smstxt.indexOf("due on "), smstxt.indexOf("due on ") + 25);
					    	   a= str1.replaceAll("[^\\d-/]", "");
					    	   
					    	}
					    	else if (smstxt.contains(" due")){     
					    	    str1 = smstxt.substring(smstxt.indexOf("due "), smstxt.indexOf("due ") + 25);
					    	   a= str1.replaceAll("[^\\d-/]", "");
					    	   
					    	}attributeValue = a;
		   }
				              
		   else if((smstxt.contains("RLICOL") || smstxt.contains("FUTGEN") ||smstxt.contains("IPRULI") || smstxt.contains("BJAZLI") || smstxt.contains("ICICIL") || smstxt.contains("ETLIFE")  || smstxt.contains("ICICIP") || smstxt.contains("ICICIL")) &&  smstxt.contains("premium") && (smstxt.contains("rs") || smstxt.contains(" rs.") || smstxt.contains("inr")) )   {
	  		
	  		 priority = 1.02;
	               
				       	                                 
	                        
			     
			             if (smstxt.contains("due on ")){
	                                  
				    	    str1 = smstxt.substring(smstxt.indexOf("due on "), smstxt.indexOf("due on ") + 25);
				    	   a= str1.replaceAll("[^\\d-/]", "");

				    	}
				    	else if (smstxt.contains("account on")){
				    	  
				    	   str1 = smstxt.substring(smstxt.indexOf("account on "), smstxt.indexOf("account on ") + 30);
				    	   a= str1.replaceAll("[^\\d-/]", "");

				    	}
					else if (smstxt.contains("due date was")){
				    	 
				    	    str1 = smstxt.substring(smstxt.indexOf("due date was"), smstxt.indexOf("due date was") + 30);
				    	   a= str1.replaceAll("[^\\d-/]", "");

				    	}
					else if (smstxt.contains("due date ")){
				    	      
				    	    str1 = smstxt.substring(smstxt.indexOf("due date "), smstxt.indexOf("due date ") + 30);
				    	   a= str1.replaceAll("[^\\d-/]", "");

				    	}
			             attributeValue=a;	
		   }
		   
		    try {
		    	
		     if (attributeValue.length() > 0) {
		      context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo  +"\t"+ userid+"\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t" +fromNumber + "\t" + Double.toString(priority) +"\t" +usercategory));
		      }
		    } catch (InterruptedException e) {
		     e.printStackTrace();
		    }
			return  String.valueOf(attributeValue);
		   }

	  public String healthinsurance_emiduedate(String smstxt, String smsId, String phoneNo,String userid, String time, String fromNumber, Context context,String operator,String circle, String usercategory) throws IOException {

		   attributeName = "healthinsurance_emiduedate";
		   attributeValue = "";
		   priority=1.01;
		   String t=time;
		   if( smstxt.contains("health")  &&  smstxt.contains("premium") && (smstxt.contains("due date ") || smstxt.contains("due on ")) && !(smstxt.contains(" gym") || smstxt.contains("hospital") )  )   {
	  		 
	  		 String a="";
	  		 String str1;
	  		
				       	                                 
	                        
			     
			             if (smstxt.contains("due on ")){
	                                    
				    	    str1 = smstxt.substring(smstxt.indexOf("due on "), smstxt.indexOf("due on ") + 25);
				    	   a= str1.replaceAll("[^\\d-/]", "");

				    	}
				    	else if (smstxt.contains("account on")){
				    	      
				    	   str1 = smstxt.substring(smstxt.indexOf("account on "), smstxt.indexOf("account on ") + 30);
				    	   a= str1.replaceAll("[^\\d-/]", "");

				    	}
					else if (smstxt.contains("due date was")){
				    	   
				    	    str1 = smstxt.substring(smstxt.indexOf("due date was"), smstxt.indexOf("due date was") + 30);
				    	   a= str1.replaceAll("[^\\d-/]", "");

				    	}
					else if (smstxt.contains("due date ")){
				    	   
			    	    str1 = smstxt.substring(smstxt.indexOf("due date "), smstxt.indexOf("due date ") + 30);
			    	   a= str1.replaceAll("[^\\d-/]", "");

			    	}

			             attributeValue=a;
	     }
			     

		    try {
		    	
		     if (attributeValue.length() > 0) {
		      context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t" +fromNumber + "\t" + Double.toString(priority) +"\t" +usercategory));
		      }
		    } catch (InterruptedException e) {
		     e.printStackTrace();
		    }
			return  String.valueOf(attributeValue);
		   }
	  
	  public String carinsurance_emiduedate(String smstxt, String smsId, String phoneNo,String userid, String time, String fromNumber, Context context,String operator,String circle, String usercategory) throws IOException {

		   attributeName = "carinsurance_emiduedate";
		   attributeValue = "";
		   priority = 1.01;
		   String t=time;
		   if( smstxt.contains(" car")  &&  smstxt.contains("premium") && (smstxt.contains("due date ") || smstxt.contains("due on ")) && !(smstxt.contains(" gym") || smstxt.contains("hospital") )  )   {
	 		 
	 		 String a="";
	 		 String str1;
				       	                                 
	                       
			     
			             if (smstxt.contains("due on ")){
	                                   
				    	    str1 = smstxt.substring(smstxt.indexOf("due on "), smstxt.indexOf("due on ") + 25);
				    	   a= str1.replaceAll("[^\\d-/]", "");

				    	}
				    	else if (smstxt.contains("account on")){
				    	      
				    	   str1 = smstxt.substring(smstxt.indexOf("account on "), smstxt.indexOf("account on ") + 30);
				    	   a= str1.replaceAll("[^\\d-/]", "");

				    	}
					else if (smstxt.contains("due date was")){
				    	   
				    	    str1 = smstxt.substring(smstxt.indexOf("due date was"), smstxt.indexOf("due date was") + 30);
				    	   a= str1.replaceAll("[^\\d-/]", "");

				    	}
					else if (smstxt.contains("due date ")){
				    	   
			    	    str1 = smstxt.substring(smstxt.indexOf("due date "), smstxt.indexOf("due date ") + 30);
			    	   a= str1.replaceAll("[^\\d-/]", "");

			    	}

			             attributeValue=a;
	    }
			     

		    try {
		    	
		     if (attributeValue.length() > 0) {
		      context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo  +"\t"+ userid+ "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t" +fromNumber + "\t" + Double.toString(priority) +"\t" +usercategory));
		      }
		    } catch (InterruptedException e) {
		     e.printStackTrace();
		    }
			return  String.valueOf(attributeValue);
		   }

	 
	  
	  public String car_emi(String smstxt, String smsId, String phoneNo,String userid, String time, String fromNumber, Context context,String operator,String circle, String usercategory) throws IOException {

		   attributeName = "car_emi";
		   attributeValue = "";
		   priority=1.01;	
		   String t=time;
		   if ((smstxt.contains(" emi") && smstxt.contains(" car")) && (smstxt.contains(" rs") ||smstxt.contains(" inr") )) {
		  		 String a="";
		  		 String str1;
		  		 String str2;
				     
				                
					    	 if (smstxt.contains(" inr")){
						       	 str1 = smstxt.substring(smstxt.indexOf("inr "), smstxt.indexOf("inr ") + 20);
						       	 str2= str1.substring(str1.indexOf("inr ")+3,str1.indexOf(" ",4));
						       	 a = str2.replaceAll("[^\\d.]", "");
					    	 		}
					        else if (smstxt.contains("inr")){
						       	 str1 = smstxt.substring(smstxt.indexOf("inr"), smstxt.indexOf("inr") + 20);
						       	 str2= str1.substring(str1.indexOf("inr")+3,str1.indexOf(" ",4));
						       	 a = str2.replaceAll("[^\\d.]", "");
					        		}				       	

					    	else if (smstxt.contains("rs.")){
						       	 str1 = smstxt.substring(smstxt.indexOf("rs."), smstxt.indexOf("rs.") + 20)	;
						       	 str2= str1.substring(str1.indexOf("rs.")+3,str1.indexOf(" ",5));
						       	 a = str2.replaceAll("[^\\d.]", "");
						       	       }
					    	else if (smstxt.contains("rs ")){
					       	 	str1 = smstxt.substring(smstxt.indexOf("rs "), smstxt.indexOf("rs ") + 20);
					       	   	str2= str1.substring(str1.indexOf("rs ")+3,str1.indexOf(" ",4));
					       	 	a = str2.replaceAll("[^\\d.]", "");
					       	       }
				                 attributeValue=a;
				       }
			     

		    try {
		    	
		     if (attributeValue.length() > 0) {
		      context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo  +"\t"+ userid+ "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t" +fromNumber + "\t" + Double.toString(priority) +"\t" +usercategory));
		      }
		    } catch (InterruptedException e) {
		     e.printStackTrace();
		    }
			return  String.valueOf(attributeValue);
		   }

	  
	  
	  
	  public String home_loanemi(String smstxt, String smsId, String phoneNo,String userid, String time, String fromNumber, Context context,String operator,String circle, String usercategory) throws IOException {

		   attributeName = "home_loanemi";
		   attributeValue = "";
		   priority = 1.01;
		   String t=time;
		   if ((smstxt.contains(" emi") &&  smstxt.contains("home loan")) && (smstxt.contains(" rs") ||smstxt.contains(" inr") )) {
		 		 String a="";
		 		 String str1;
		 		 String str2;
				  
		 		 			if (smstxt.contains("inr "))
		 		 				{
						       	 str1 = smstxt.substring(smstxt.indexOf("inr "), smstxt.indexOf("inr ") + 20);
						       	 str2= str1.substring(str1.indexOf("inr ")+3,str1.indexOf(" ",4));
						       	 a = str2.replaceAll("[^\\d.]", "");
						       	}
					        else if (smstxt.contains("inr")){
						       	 str1 = smstxt.substring(smstxt.indexOf("inr"), smstxt.indexOf("inr") + 20);
						       	 str2= str1.substring(str1.indexOf("inr")+3,str1.indexOf(" ",4));
						       	 a = str2.replaceAll("[^\\d.]", "");
						       	 }				       	

					    	else if (smstxt.contains("rs.")){
						       	 str1 = smstxt.substring(smstxt.indexOf("rs."), smstxt.indexOf("rs.") + 20)	;
						       	 str2= str1.substring(str1.indexOf("rs.")+3,str1.indexOf(" ",4));
						       	 a = str2.replaceAll("[^\\d.]", "");
					    		}
					    	else if (smstxt.contains("rs ")){
					       	 	str1 = smstxt.substring(smstxt.indexOf("rs "), smstxt.indexOf("rs ") + 20);
					       	   	str2= str1.substring(str1.indexOf("rs ")+3,str1.indexOf(" ",4));
					       	 	a = str2.replaceAll("[^\\d.]", "");
					    		}
				                 attributeValue=a;
				       }
			     

		    try {
		    	
		     if (attributeValue.length() > 0) {
		      context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t" +fromNumber + "\t" + Double.toString(priority) +"\t" +usercategory));
		      }
		    } catch (InterruptedException e) {
		     e.printStackTrace();
		    }
			return  String.valueOf(attributeValue);
		   }
	  
	  public String lifeinsurance_emi(String smstxt, String smsId, String phoneNo,String userid, String time, String fromNumber, Context context,String operator,String circle, String usercategory) throws IOException {

		   attributeName = "lifeinsurance_emi";
		   attributeValue = "";
		   priority=1.01;
		   String t=time;
		   if((fromNumber.equals("RLICOL") || fromNumber.equals("FUTGEN") ||fromNumber.equals("IPRULI") || fromNumber.equals("BJAZLI") || fromNumber.equals("ICICIL") || fromNumber.equals("ETLIFE")  || fromNumber.equals("ICICIP") || fromNumber.equals("ICICIL"))
	 &&  smstxt.contains("premium") && (smstxt.contains(" rs") || smstxt.contains(" rs.")||smstxt.contains(" rs") ||smstxt.contains(" inr") ||smstxt.contains(" rs.")) )   {
	  		 String a="";
	  		 String str1;
	  		 String str2;
	               
				       	                                 
	                        
			     
			              if (smstxt.contains(" rs.")){

				    	    str1 = smstxt.substring(smstxt.indexOf("rs."), smstxt.indexOf("rs.") + 20);
				    	   str2= str1.substring(str1.indexOf("rs.")+3,str1.indexOf(" ",4));
				    	   a= str2.replaceAll("[^\\d.]", "");
				    	                          }
				    
				    	else if (smstxt.contains("inr ")){
					       	 str1 = smstxt.substring(smstxt.indexOf("inr "), smstxt.indexOf("inr ") + 20);
					       	   str2= str1.substring(str1.indexOf("inr ")+3,str1.indexOf(" ",4));
					       	 a = str2.replaceAll("[^\\d.]", "");
					       	}
						else if (smstxt.contains("inr")){
					       	 str1 = smstxt.substring(smstxt.indexOf("inr"), smstxt.indexOf("inr") + 20);
					       	   str2= str1.substring(str1.indexOf("inr")+3,str1.indexOf(" ",4));
					       	 a = str2.replaceAll("[^\\d.]", "");
	                                       
	                                       }
				    	else if (smstxt.contains("rs.")){
					       	 str1 = smstxt.substring(smstxt.indexOf("rs."), smstxt.indexOf("rs.") + 20)	;
					       	   str2= str1.substring(str1.indexOf("rs.")+3,str1.indexOf(" ",5));
					       	 a = str2.replaceAll("[^\\d.]", "");
	                                       
	  	                                                        }
				    	else if (smstxt.contains("rs ")){
				       	 str1 = smstxt.substring(smstxt.indexOf("rs "), smstxt.indexOf("rs ") + 20);
				       	   str2= str1.substring(str1.indexOf("rs ")+3,str1.indexOf(" ",4));
				       	 a = str2.replaceAll("[^\\d.]", "");
	                               
				       	                                 }
			              attributeValue=a;	
	     }
			     

		    try {
		    	
		     if (attributeValue.length() > 0) {
		      context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo  +"\t"+ userid+ "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t" +fromNumber + "\t" + Double.toString(priority) +"\t" + usercategory));
		      }
		    } catch (InterruptedException e) {
		     e.printStackTrace();
		    }
			return  String.valueOf(attributeValue);
		   }

	  public String healthinsurance_emi(String smstxt, String smsId, String phoneNo,String userid, String time, String fromNumber, Context context,String operator,String circle, String usercategory) throws IOException {

		   attributeName = "healthinsurance_emi";
		   attributeValue = "";
		   priority = 1.01;
		   String t=time;
		   if( smstxt.contains("health")  &&  smstxt.contains("premium") && (smstxt.contains(" rs") || smstxt.contains(" rs.")||smstxt.contains(" rs") ||smstxt.contains(" inr") ||smstxt.contains(" rs. ")) && !(smstxt.contains(" gym") || smstxt.contains("hospital") )  )   {
	  		 String a="";
	  		 String str1;
	  		 String str2;
	               
				       	                                 
	                        
			     
			              if (smstxt.contains("rs.")){

				    	    str1 = smstxt.substring(smstxt.indexOf("rs."), smstxt.indexOf("rs.") + 20);
				    	   str2= str1.substring(str1.indexOf("rs.")+3,str1.indexOf(" ",4));
				    	   a= str2.replaceAll("[^\\d.]", "");
				    	                          }
				    	
				    	else if (smstxt.contains("inr ")){
					       	 str1 = smstxt.substring(smstxt.indexOf("inr "), smstxt.indexOf("inr ") + 20);
					       	   str2= str1.substring(str1.indexOf("inr ")+3,str1.indexOf(" ",4));
					       	 a = str2.replaceAll("[^\\d.]", "");
					       	}
						else if (smstxt.contains("inr")){
					       	 str1 = smstxt.substring(smstxt.indexOf("inr"), smstxt.indexOf("inr") + 20);
					       	   str2= str1.substring(str1.indexOf("inr")+3,str1.indexOf(" ",4));
					       	 a = str2.replaceAll("[^\\d.]", "");
	                               
	                                       }
				    	else if (smstxt.contains("rs.")){
					       	 str1 = smstxt.substring(smstxt.indexOf("rs."), smstxt.indexOf("rs.") + 20)	;
					       	   str2= str1.substring(str1.indexOf("rs.")+3,str1.indexOf(" ",5));
					       	 a = str2.replaceAll("[^\\d.]", "");
	                               
	  	                                                        }
				    	else if (smstxt.contains("rs ")){
				       	 str1 = smstxt.substring(smstxt.indexOf("rs "), smstxt.indexOf("rs ") + 20);
				       	   str2= str1.substring(str1.indexOf("rs ")+3,str1.indexOf(" ",4));
				       	 a = str2.replaceAll("[^\\d.]", "");
	                               
				       	                                 }
			              attributeValue =a;	
	     }
			     

		    try {
		    	
		     if (attributeValue.length() > 0) {
		      context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo  +"\t"+ userid+ "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t" +fromNumber + "\t" + Double.toString(priority) +"\t" +usercategory));
		      }
		    } catch (InterruptedException e) {
		     e.printStackTrace();
		    }
			return  String.valueOf(attributeValue);
		   }

	  public String carinsurance_emi(String smstxt, String smsId, String phoneNo,String userid, String time, String fromNumber, Context context,String operator,String circle, String usercategory) throws IOException {

		   attributeName = "carinsurance_emi";
		   attributeValue = "";
		   priority=1.01;
		   String t=time;
		   if( smstxt.contains(" car")  &&  smstxt.contains("premium") && (smstxt.contains(" rs") || smstxt.contains(" rs.")||smstxt.contains(" rs") ||smstxt.contains(" inr") ||smstxt.contains(" rs.")) && !(smstxt.contains(" gym") || smstxt.contains("hospital") )  )   {
	 		 String a="";
	 		 String str1;
	 		 String str2;
	              

	         if (smstxt.contains("rs.")){

	   	    str1 = smstxt.substring(smstxt.indexOf("rs."), smstxt.indexOf("rs.") + 20);
	   	   str2= str1.substring(str1.indexOf("rs.")+3,str1.indexOf(" ",5));
	   	   a= str2.replaceAll("[^\\d.]", "");
	   	   
	   	                          }
	   
	        else if (smstxt.contains("inr ")){
		       	 str1 = smstxt.substring(smstxt.indexOf("inr "), smstxt.indexOf("inr ") + 20);
		       	   str2= str1.substring(str1.indexOf("inr ")+3,str1.indexOf(" ",4));
		       	 a = str2.replaceAll("[^\\d.]", "");
		       	}
			else if (smstxt.contains("inr")){
		       	 str1 = smstxt.substring(smstxt.indexOf("inr"), smstxt.indexOf("inr") + 20);
		       	   str2= str1.substring(str1.indexOf("inr")+3,str1.indexOf(" ",4));
		       	 a = str2.replaceAll("[^\\d.]", "");
	                 
	                         }
			else if (smstxt.contains("rs ")){
	      	 str1 = smstxt.substring(smstxt.indexOf("rs "), smstxt.indexOf("rs ") + 20);
	      	   str2= str1.substring(str1.indexOf("rs ")+3,str1.indexOf(" ",4));
	      	 a = str2.replaceAll("[^\\d.]", "");
	                 
	      	                                 }
			              attributeValue =a;	
	    }
			     

		    try {
		    	
		     if (attributeValue.length() > 0) {
		      context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo  +"\t"+ userid+ "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t" +fromNumber + "\t" + Double.toString(priority) +"\t" +usercategory));
		      }
		    } catch (InterruptedException e) {
		     e.printStackTrace();
		    }
			return  String.valueOf(attributeValue);
		   }

	   //todo need to review and add more rules
	   public String savings_account(String smstxt, String smsId, String phoneNo,String userid, String time, Context context,String operator,String circle,String fromNumber, String usercategory) throws IOException {

	    attributeName = "saving_account";
	    attributeValue = "";

	    if (smstxt.contains("savings a/c") || smstxt.contains("savings acct") || smstxt.contains("savings no") || smstxt.contains("saving account") || smstxt.contains("savings acct") || smstxt.contains("savings transactions") || smstxt.contains("savings available balance") || smstxt.contains("salary") && (smstxt.contains("credited") || smstxt.contains("deposited") || smstxt.contains("debited"))) {
	     priority = 1.01;
	     attributeValue = "Yes";
	    } 

	    else if(smstxt.contains("net banking") || smstxt.contains("netbanking") || smstxt.contains("credit card") || smstxt.contains("paytm wallet") || smstxt.contains("internet banking") || smstxt.contains("bankbook") )
	    {
	    	priority = 1.02;
	        attributeValue = "Yes";
	    }

	    else if (smstxt.contains("stock statement") || ((smstxt.contains("demat") || smstxt.contains("stock")) && (smstxt.contains("account") || smstxt.contains(" ac") || smstxt.contains(" a/c")) ) )
	    {
	    	priority = 1.03;
	    	attributeValue = "Yes";
	    	
	    }

	    else if(smstxt.contains("mutual fund") || smstxt.contains("capital market") ||  smstxt.contains("sharekhan") || smstxt.contains(" sip") || smstxt.contains("folio no"))
	    {
	    	priority = 1.04;
	        attributeValue = "Yes";
	    }
	    else if (fromNumber.contains("WIPRO") || fromNumber.contains("ORACLE"))
	    {
	    	priority = 1.05;
	    	attributeValue = "Yes";
	    }
	    else if(smstxt.contains("credit card") || smstxt.contains("home loan") || smstxt.contains("mutual fund"))
	    {
	    	priority = 1.06;
	    	attributeValue = "Yes";
	    }
	 else if (smstxt.contains("redeem") &&  smstxt.contains("points") )   { 
	     priority = 1.07;
	     attributeValue = "Yes";
	    }
	    else if (smstxt.contains("nifty") || smstxt.contains(" nse") || smstxt.contains("sensex")  )   { 
	        priority = 1.08;
	        attributeValue = "Yes";
	       }
	    else if (smstxt.contains("term deposit")  )   { 
	        priority = 1.09;
	        attributeValue = "Yes";
	       }
	    
	 else if (smstxt.contains("eligible" )&& (smstxt.contains(" loan") || smstxt.contains("credit card"))  )   { 
	        priority = 2.02;
	        attributeValue = "Yes";
	       }  
	 else if (fromNumber.contains("GOIBIBO") || smstxt.contains("gocash"))
	 {
		 priority = 3.01;
		 attributeValue = "Yes";
	 }
	   else if ((smstxt.contains(" bank") && smstxt.contains("debit")) ) {
	     priority = 4.01; // to do
	     attributeValue = "Yes";
	    } else if ((smstxt.contains(" bank") && smstxt.contains(" otp")) ) {
	     priority = 4.02; // to do
	     attributeValue = "Yes";
	    } else if (smstxt.contains("credited") && !smstxt.contains("debited") ) {
	     priority = 4.03; // to do
	     attributeValue = "Yes";
	    } else if (smstxt.contains("netbanking") || smstxt.contains("internet banking") ){
	     priority = 4.04; // to do
	     attributeValue = "Yes";
	    } else if (smstxt.contains("remittance account rejected") || smstxt.contains("last ") && smstxt.contains(" txn")  ) {
	     priority = 4.05; // to do
	     attributeValue = "Yes";
	    }

	    try {
	    	
	     if (attributeValue.length() > 0) {
	      context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t"  +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
	     }
	    } catch (InterruptedException e) {
	     e.printStackTrace();
	    }
		return  String.valueOf(attributeValue);
	   }

	   
	   public String has_kids(String smstxt, String smsId, String phoneNo,String userid, String time, String fromNumber, Context context,String operator,String circle, String usercategory) throws IOException {
	    attributeName = "has_kids";
	    attributeValue = "";
	    String t=time;
	    try {
	    	
	    if (smstxt.contains("parent") ||smstxt.contains("parents")|| smstxt.contains("your child") || smstxt.contains("for kids") || smstxt.contains("quarter fee") || smstxt.contains("your ward")) {
	     priority = 1.01;
	     attributeValue = "Yes";
	    } 
	    if (smstxt.contains("dear parent")||smstxt.contains("hello parent")||smstxt.contains("hi parent") ||smstxt.contains("dear parents") ) {
	        priority = 1.01;
	        attributeValue = "Yes";
	       }
	   
	    
	    else if ((smstxt.contains("diaper") ||smstxt.contains("bassinet") ||smstxt.contains(" crib") ||smstxt.contains(" baby") || smstxt.contains("huggies") ||smstxt.contains("pampers")) && (smstxt.contains("ordered") || smstxt.contains("delivered"))   )
	    {
	         priority = 1.02;
	         attributeValue = "Yes";
	    }
	    else if (fromNumber.equals("arkay1")||fromNumber.equals("vmkvec1") ||fromNumber.equals("SomerN")
				||fromNumber.equals("SVCHSMTNL")||fromNumber.equals("innovatorysoln")||
				fromNumber.equals("britishschoolx")||fromNumber.equals("JCET1")||fromNumber.equals("mamce1")||
				fromNumber.equals("OXFORD1")||fromNumber.equals("SAISUDHIR1")||
				fromNumber.equals("sacet1")||fromNumber.equals("pec")||fromNumber.equals("LAQSHYA1")||fromNumber.equals("kstechcri")||
				fromNumber.equals("bridlemf1")||fromNumber.equals("LAQSHYA1")||fromNumber.equals("kstechcri")||
				fromNumber.equals("Somervillexml")||fromNumber.equals("nirmalbhartia")||fromNumber.equals("vivekaschool")||
				fromNumber.equals("jubileexml")||fromNumber.equals("loyola")||fromNumber.equals("yuvabharathi") ||
				fromNumber.equals("salwanschool")||fromNumber.equals("SUNCITY") ||
				fromNumber.equals("VMOUKOTA")||fromNumber.equals("chrysaliseduc") ||
				fromNumber.equals("hanuman")||fromNumber.equals("triesten4") ||
				fromNumber.equals("Zipcash")||fromNumber.equals("davisk") ||
				fromNumber.equals("kstechcri")||fromNumber.equals("kstechcri"))
	    {
	    	   priority = 1.03;
	           attributeValue = "Yes";	
	    }
	    
	    else if (smstxt.contains("baby of") && (smstxt.contains("your payment") || smstxt.contains("your appointment") ||  (smstxt.contains(" added") && smstxt.contains(" plan")) ) ){
	        priority = 2.01;
	        attributeValue = "Yes";
	    }
	    
	     if (attributeValue.length() > 0) {
	      context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo  +"\t"+ userid+ "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t"   +fromNumber + "\t"+ Double.toString(priority) +"\t" +usercategory));
	         }
	    } catch (InterruptedException e) {
	     e.printStackTrace();
	    }
		return  String.valueOf(attributeValue);
	   }


	   public String income(String smstxt, String smsId, String phoneNo,String userid, String t, String fromNumber, Context context,String operator,String circle, String usercategory) throws IOException {
	    String s="";
	    int salaryamt;
	    //String attributeName = "income";
	    attributeValue = "";
	    try {
	 	 
	     if (fromNumber.equals("ICICIB"))
	     {
	     if (smstxt.contains("salary") && smstxt.contains("is credited with inr")) {
	      priority = 1.00;
	      s = smstxt.substring(smstxt.indexOf("is credited with inr") + 20, smstxt.indexOf(" on"));
	    
	     }
	    } 
	     else if (fromNumber.equals("DENABK")) {
	     if (smstxt.contains("salary") && smstxt.contains("is credited with rs")) {
	      priority = 1.00;
	      s = smstxt.substring(smstxt.indexOf("is credited with rs.") + 20, smstxt.indexOf(" towards"));
	     
	     }
	    } else if (fromNumber.equals("CorpBk")) {
	     if (smstxt.contains("credited with salary inr ")) {
	      priority = 1.00;
	      s = smstxt.substring(smstxt.indexOf("credited with salary inr ") + 25, smstxt.indexOf(" on"));
	    
	     }
	    } else if (fromNumber.equals("LVBANK")) {
	     if (smstxt.contains("credited with rs.") && smstxt.contains("net salary")) {
	      priority = 1.00;
	      s = smstxt.substring(smstxt.indexOf("credited with rs.") + 17, smstxt.indexOf(" on"));
	      
	     }
	    }
	    if (fromNumber.equals("BMCBNK")) {
	     if (smstxt.contains(" has been credited by inr ") && smstxt.contains("info- by salary")) {
	      priority = 1.00;
	      s = smstxt.substring(smstxt.indexOf("has been credited by inr ") + 25, smstxt.indexOf(" on"));

	  

	     }
	    } else if (fromNumber.equals("PMCBnk")) {
	     if (smstxt.contains("ecs of inr ") && smstxt.contains("trust-salary")) {
	      priority = 1.00;
	      s = smstxt.substring(smstxt.indexOf("ecs of inr ") + 11, smstxt.indexOf(" from"));
	  
	     }
	    }

	    if (fromNumber.equals("TJSBNK")) {
	     if (smstxt.contains("credited") && smstxt.contains("salaryorigbrcd")) {
	      priority = 1.00;
	      //s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
	      s = smstxt.substring(11, smstxt.indexOf(" (ref no-  )"));

	    


	     }
	    } else if (fromNumber.equals("NKGSB")) {
	     if (smstxt.contains("salary") && smstxt.contains("credited")) {
	      priority = 1.00;
	      //s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
	      s = smstxt.substring(37, smstxt.indexOf(" on"));
	 

	     }
	    }

	    if (fromNumber.equals("RSBANK")) {
	     if (smstxt.contains("salary") && smstxt.contains("credited")) {
	      priority = 1.00;
	      //s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
	      s = smstxt.substring(38, smstxt.indexOf(" on"));
	 


	     }
	    } else if (fromNumber.equals("ACEBNK")) {
	     if (smstxt.contains("salary") && smstxt.contains("credited")) {
	      priority = 1.00;
	      //s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
	      s = smstxt.substring(38, smstxt.indexOf(" on"));
	  }
	    } else if (fromNumber.equals("GPPJSB")) {
	     if (smstxt.contains("salary") && smstxt.contains("credited")) {
	      priority = 1.00;
	      s = smstxt.substring(39, smstxt.indexOf(" on"));
	  }
	    }

	    if (fromNumber.equals("NNSBANK")) {
	     if (smstxt.contains("salary") && smstxt.contains("credited")) {
	      priority = 1.00;
	      
	      s = smstxt.substring(65, smstxt.indexOf(" on"));
	 


	     }
	    } else if (fromNumber.equals("SUDICO")) {
	     if (smstxt.contains("salary") && smstxt.contains("credited")) {
	      priority = 1.00;
	       s = smstxt.substring(37, smstxt.indexOf(". your"));
	 

	     }
	    } else if (fromNumber.equals("MCCDAY")) {
	     if (smstxt.contains("salary") && smstxt.contains("credited")) {
	      priority = 1.00;
	      s = smstxt.substring(22, smstxt.indexOf(" for"));
	 

	     }
	    } else if (fromNumber.equals("BCCBNK")) {
	     if (smstxt.contains("salary") && smstxt.contains("credited")) {
	      priority = 1.00;
	      s = smstxt.substring(46, smstxt.indexOf(" on"));
	 

	     }
	    } else if (fromNumber.equals("SSBMUM")) {
	     if (smstxt.contains("salary") && smstxt.contains("credited")) {
	      priority = 1.00;
	      s = smstxt.substring(39, smstxt.indexOf(" On"));
	 
	     }
	    } else if (fromNumber.equals("BLBNK")) {
	     if (smstxt.contains("salary") && smstxt.contains("credited")) {
	      priority = 1.00;
	      s = smstxt.substring(54, smstxt.indexOf(" on"));
	 
	     }
	    } else if (fromNumber.equals("PNB")) {
	     if (smstxt.contains("salary") && smstxt.contains("credited")) {
	      priority = 1.00;
	      s = smstxt.substring(37, smstxt.indexOf(","));
	 

	     }
	    } else if (fromNumber.equals("VBANK")) {
	     if (smstxt.contains("salary") && smstxt.contains("credited")) {
	      priority = 1.00;
	      s = smstxt.substring(29, smstxt.indexOf(" on"));
	 

	     }
	    } else if (fromNumber.equals("UBANK")) {
	     if (smstxt.contains("salary") && smstxt.contains("credited")) {
	      priority = 1.00;
	      s = smstxt.substring(40, smstxt.indexOf(" towards"));
	 

	     }
	    } else if (fromNumber.equals("TBSREW")) {
	     if (smstxt.contains("salary") && smstxt.contains("credited")) {
	      priority = 1.00;
	      s = smstxt.substring(35, smstxt.indexOf(" has"));
	     
	 
	     }
	    }
	    else if (fromNumber.equals("AxisBk")) {
	     if (smstxt.contains("salary") || smstxt.contains("credited")) {
	      priority = 1.00;
	      s = smstxt.substring(smstxt.indexOf("is credited rs ") + 15, smstxt.indexOf(" on"));
	 
	     }
	    }
	    else if (smstxt.contains("credited with ") && smstxt.contains("salary")){
			smstxt=smstxt.replace(".", ",");
			if (smstxt.contains("inr")) {
				String s1 = smstxt.substring(smstxt.indexOf("inr"),smstxt.length());
				String smslist[] = new String[20];
				 smslist = s1.split(" ");
				 priority = 1.00;
				s = smslist[1].replace(",",".");
				 s=s.substring(0,s.indexOf("."));
				
				   
										}
			else if (smstxt.contains("rs")) {
				String s1 = smstxt.substring(smstxt.indexOf("rs"),smstxt.length());
				String smslist[] = new String[20];
				 smslist = s1.split(" ");
				 priority = 1.00;
				 s = smslist[1].replace(",",".");
				 s=s.substring(0,s.indexOf("."));
				  

					}
	    }
	    else if (fromNumber.equals("PNBCRC")) {
		     if (smstxt.contains("credit limit") && smstxt.contains("changed from") ) {
		      priority = 2.01;
		      s = smstxt.substring(smstxt.indexOf("changed from")+1,smstxt.length());
		      String a = s.substring(s.indexOf("to")+6,s.length()-4);
		             s= a.replaceAll(",", "");
		             double sal=0;
		             int c=0;
		               c=Integer.parseInt(a);

		                sal= (c/ 1.3);
		              
		                sal=sal*12;
		         	   if ((sal<= 300000 )){ 
					       	 attributeValue ="0-3L";
					       
					                                          }
		         	   else if ((sal> 300000 )&& (sal<600000)){ 
		            	 attributeValue ="3-6L";
		                                           }
		             else if ((sal> 600000 )&& (sal<1000000)){
		            	 attributeValue ="6-10L";
		                                           }
		             else if ((sal> 1000000) && (sal<1500000)){
		            	 attributeValue ="10-15L";
		                                        }
		                                           
		             else if ((sal> 1500000) && (sal<2000000)){
		            	 attributeValue ="15-20L";
		                                           }
		             else if ((sal>2000000) && (sal< 2500000))
		             {
		             	attributeValue = "20-25L";
		             }
		             else if ((sal> 2500000) && (sal<4000000)){
		            	 attributeValue ="25-40L";
		             }
		             else if ((sal> 4000000) && (sal<6000000)){
		            	 attributeValue ="40-60L";
		             }
		             else if ((sal> 6000000) && (sal<10000000)){
		            	 attributeValue ="60L-1CR";
		             }
		             else{
		            	 attributeValue ="1CR+";

		                                           }                  }
		     }
				

	    else if (smstxt.contains("credit limit") && smstxt.contains("changed from"))
		{ priority = 2.01;    
		 if (smstxt.contains(" inr.") )
		{	
			s = smstxt.substring(smstxt.indexOf("inr")+4,smstxt.length());
			String c = s.substring(s.indexOf("to")+7,s.length()).replace(".","");
			String d = c.replaceAll("[^\\d.]", "");
			double sal;
			int j =0;
			j = Integer.parseInt(d);
			sal = j*70/100;
		
			  sal=sal*12;
				
			   if ((sal<= 300000 )){ 
			       	 attributeValue ="0-3L";
			       	
			                                          }
			   else  if ((sal> 300000 )&& (sal<600000)){ 
	       	 attributeValue ="3-6L";
	     
	                                          }
	            else if ((sal> 600000 )&& (sal<1000000)){
	       	 attributeValue ="6-10L";
	           	 
	                                          }
	            else if ((sal> 1000000) && (sal<1500000)){
	       	 attributeValue ="10-15L";
	           	
	                                       }
	                                          
	            else if ((sal> 1500000) && (sal<2000000)){
	       	 attributeValue ="15-20L";
	           	
	                                          }
	            else if ((sal>2000000) && (sal< 2500000))
	            {
	            	attributeValue = "20-25L";
	            }
	         else if ((sal> 2500000) && (sal<4000000)){
	           	 attributeValue ="25-40L";
	           	
	            }
	            else if ((sal> 4000000) && (sal<6000000)){
	       	 attributeValue ="40-60L";
	           	
	            }
	            else if ((sal> 6000000) && (sal<10000000)){
	   	 attributeValue ="60L-1CR";
	           	
	            }
	            else{
	       	 attributeValue ="1CR+";
	           	

	                                          }                  }
	    }
	  if (smstxt.contains("credit limit") && smstxt.contains("changed from"))
		{     priority = 2.01;    
		 if (smstxt.contains(" rs.") )
		{	
			s = smstxt.substring(smstxt.indexOf("rs")+3,smstxt.length());
			String c = s.substring(s.indexOf("to")+6,s.length()).replace(".","");
			String d = c.replaceAll("[^\\d.]", "");
			double sal;
			int j =0;
			j = Integer.parseInt(d);
			sal = j*70/100;
		
			  sal=sal*12;
			   if ((sal<= 300000 )){ 
			       	 attributeValue ="0-3L";
			       
			                                          }	
	         
			   else if ((sal> 300000 )&& (sal<600000)){ 
	       	 attributeValue ="3-6L";
	     
	                                          }
	            else if ((sal> 600000 )&& (sal<1000000)){
	       	 attributeValue ="6-10L";
	           	 
	                                          }
	            else if ((sal> 1000000) && (sal<1500000)){
	       	 attributeValue ="10-15L";
	           	
	                                       }
	                                          
	            else if ((sal> 1500000) && (sal<2000000)){
	       	 attributeValue ="15-20L";
	           	
	                                          }
	            else if ((sal>2000000) && (sal< 2500000))
	            {
	            	attributeValue = "20-25L";
	            }
	         else if ((sal> 2500000) && (sal<4000000)){
	           	 attributeValue ="25-40L";
	           	
	            }
	            else if ((sal> 4000000) && (sal<6000000)){
	       	 attributeValue ="40-60L";
	           	
	            }
	            else if ((sal> 6000000) && (sal<10000000)){
	   	 attributeValue ="60L-1CR";
	           	
	            }
	            else{
	       	 attributeValue ="1CR+";
	           	

	                                          }                  }
	    }
	else  if (((smstxt.contains("viewed") && smstxt.contains("contact")) || (smstxt.contains("expressed") && smstxt.contains("interest"))) && (smstxt.contains("1bhk") || smstxt.contains("2bhk") || smstxt.contains("3bhk") || smstxt.contains("builder floor") || smstxt.contains(" shop")) ) {

	priority = 2.02;  // to do 
	attributeValue="10-15L";


	}     	 
	    
	    if (tryParseInt(s) && priority==1.00){
	    
	  	  salaryamt = Integer.parseInt(s);
	   
	      attributeValue = salaryRange(salaryamt);
	      	
	    }
	    else if(tryParseInt(s)){
	    	int d;
	    	d=Integer.parseInt("s");
	    	if (d>15000){
	    		salaryamt = Integer.parseInt(s);
	    		   
	    	      attributeValue = salaryRange(salaryamt);
	    	     			
	    	}
	    }
	    
	     if (attributeValue.length() > 0) {
	      context.write(new Text(smsId+"-"+"income"+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo  +"\t"+ userid+ "\t" + t + "\t" + "income"+ "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t"   +fromNumber + "\t" + Double.toString(priority) +"\t" +usercategory));
	       }
	  
	    }catch (InterruptedException e) {
	     e.printStackTrace();
	    }
		return  String.valueOf(attributeValue);
	   }

	   public String salaryRange(int salary) {
	    int ctc = salary * 12;
	    if (ctc >= 0 && ctc <= 300000) {
	     return "0-3L";
	    } else if (ctc > 300000 && ctc <= 600000) {
	     return "3-6L";
	    } else if (ctc > 600000 && ctc <= 1000000) {
	     return "6-10L";
	    } else if (ctc > 1000000 && ctc <= 1500000) {
	     return "10-15L";
	    } else if (ctc > 1500000 && ctc <= 2000000)
	     return "15-20L";
	    else if (ctc > 2000000 && ctc <= 2500000)
	     return "20-25L";
	    else if (ctc > 2500000 && ctc <= 4000000)
	     return "25-40L";
	    else if (ctc > 4000000 && ctc <= 6000000)
	     return "40-60L";
	    else if (ctc > 6000000 && ctc <= 10000000)
	     return "60L-1CR";
	    else
	     return "1CR+";
	   }

	   //todo need to review and add more rules
	   public String life_insurance(String smstxt, String smsId, String phoneNo,String userid, String time, String fromNumber, Context context,String operator,String circle,String usercategory) throws IOException {
	    attributeName = "life_insurance";
	    attributeValue = "";
	    
	    if (smstxt.contains("life insurance") || smstxt.contains("life policy") || smstxt.contains("pru policy") || smstxt.contains("indiafirst") && !(smstxt.contains("thank you for calling "))) {
	     priority = 1.01;
	     attributeValue = "Yes";
	    }
	    else if (smstxt.contains("premium")&& !(smstxt.contains(" car")||smstxt.contains("health"))){
	    	   priority = 2.01;
	    	   attributeValue = "Yes";
	    }
	    if (fromNumber.equals("SUDLIF")) {
	     priority = 3.01;
	     attributeValue = "Yes";
	    }
	    else if (smstxt.contains("premium") && !(smstxt.contains(" home") || smstxt.contains(" car")|| smstxt.contains(" gold")|| smstxt.contains("tractor"))){        //assumption
	   	 priority = 4.01;
	        attributeValue = "Yes";
	   }
	    try {
	    	
	     if (attributeValue.length() > 0){
	      context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue +"\t" +circle+ "\t" + operator  +"\t"  +fromNumber + "\t"  + Double.toString(priority) +"\t" +usercategory)); 
	     }
	    }catch (InterruptedException e) {
	     e.printStackTrace();
	    }
		return  String.valueOf(attributeValue);
	   }
	  

	   //todo need to review and add more rules
	   public String car_insurance(String smstxt, String smsId, String phoneNo,String userid, String time, String fromNumber, Context context,String operator,String circle,String usercategory) throws IOException {

	    attributeName = "car_insurance";
	    attributeValue = "";
	    String a="";
	    List <String> pat = new ArrayList<String>();
	    if ((smstxt.contains("car insurance")) || (smstxt.contains("car") && smstxt.contains("insurance")) && (smstxt.contains("policy")) && (smstxt.contains(" rs") || smstxt.contains(" inr") || smstxt.contains("due for") || smstxt.contains("due on")))
	    {
	    	   priority = 1.01;
	    	     attributeValue = "Yes";
	    }
	    else  if (smstxt.contains("maruti insurance") ) {
	     priority = 1.02;
	     attributeValue = "Yes";
	    }
	  
	    else if (smstxt.contains("vehicle") && smstxt.contains("reg. no.") && (smstxt.contains(" car") || smstxt.contains("hyundai") || smstxt.contains(" ford") || smstxt.contains("maruti") || smstxt.contains("honda")  || smstxt.contains(" fiat") || smstxt.contains("renault") || smstxt.contains(" tata") || smstxt.contains("toyota") || smstxt.contains(" bmw") || smstxt.contains("nissan") || smstxt.contains("mercedez") || smstxt.contains(" audi") || smstxt.contains("chevrolet") || smstxt.contains("volkswagen") ) && fromNumber.equals("HDFERGO")) {
	     priority = 1.03;
		    pat.add(smstxt);
		    for (String s: pat)
		    {
			Pattern p = Pattern.compile("(([A-Za-z]){2,3}(|-)(?:[0-9]){1,2}(|-)(?:[A-Za-z]){2}(|-)([0-9]){1,4})");
			Matcher m = p.matcher(s);
			while (m.find()) 
			{
				a= m.group();
	        break;
			}
		 }
	     attributeValue = "Yes";
	    }
	    else if (smstxt.contains("interested") && (smstxt.contains("in yours") || smstxt.contains("in your")) && fromNumber.equals("CARWAL") ) {
	     priority = 2.02;
	     attributeValue = "Yes";
	    }
	    else if ( ( smstxt.contains("ordered") || smstxt.contains("delivered") || smstxt.contains("dispatched") || smstxt.contains("order no") || ( (smstxt.contains("your") || smstxt.contains("yours")) && smstxt.contains("package") )  ) && smstxt.contains(" car") ) {
	     priority = 3.01;
	     attributeValue = "Yes";
	    }
	    try {
	    	
	     if (attributeValue.length() > 0){
	      context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue +"\t" +circle+ "\t" + operator  +"\t"  +fromNumber + "\t" + Double.toString(priority) + "\t" +usercategory));
	     } 
	     } catch (InterruptedException e) {
	     e.printStackTrace();
	    }
		return  String.valueOf(attributeValue);
	   }

	  
	   public String apparel(String smstxt, String smsId, String phoneNo,String userid, String time, Context context,String operator,String circle,String fromNumber, String usercategory) throws IOException {

	    attributeName = "apparel";
	    attributeValue = "";
	     try {
	    	  
	    	 if ((smstxt.contains("t-shirt") || smstxt.contains(" polos ") ||smstxt.contains(" condom") ||smstxt.contains("for men") || smstxt.contains(" sherwani") || (smstxt.contains(" shirt") && smstxt.contains(" men"))  || smstxt.contains(" sweater") || smstxt.contains(" jacket") || smstxt.contains("sweatshirt") || (smstxt.contains(" trouser") && smstxt.contains(" men")) || smstxt.contains(" jeans") || smstxt.contains(" boxer") || smstxt.contains(" loafers") || smstxt.contains(" jerseys") || smstxt.contains("cufflinks") || smstxt.contains("tie pin")  || smstxt.contains("shoes")  || smstxt.contains("boot") || smstxt.contains("sneaker") || smstxt.contains("loafer") || smstxt.contains("watches")  || smstxt.contains("belts") || smstxt.contains("blazer")) && (smstxt.contains("delivered") || smstxt.contains("ordered") || smstxt.contains("placed") || smstxt.contains("dispatched") || smstxt.contains("order no.") || smstxt.contains(" order") || smstxt.contains(" shipped") || smstxt.contains(" received")) ) {
	     priority = 1.01;
	     attributeValue = "Yes";
	    
	     context.write(new Text(smsId+"-"+"gender"+"-"+"male"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "gender" + "\t" + "male" + "\t" +circle+ "\t" + operator  +  "\t"+fromNumber + "\t" + Double.toString(priority) + "\t" +usercategory));
	     context.write(new Text(smsId+"-"+"freq_shopper"+"-"+"maybe"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "freq_shopper" + "\t" + "maybe" + "\t" +circle+ "\t" + operator  +"\t" +fromNumber + "\t"  + Double.toString(priority)+ "\t" +usercategory));

	   
	    }
	     if ( (smstxt.contains("for women") || smstxt.contains("top") || smstxt.contains("dress") || smstxt.contains("skirt")    || smstxt.contains("jeans") || smstxt.contains("skirt") || smstxt.contains("leggings") || smstxt.contains("jegging") || smstxt.contains("tunic") || smstxt.contains("kurti") || smstxt.contains("suit") || smstxt.contains("saree") || smstxt.contains("salwar") || smstxt.contains("churidar") || smstxt.contains("bags") || smstxt.contains("clutches") || smstxt.contains("totes") || smstxt.contains("sandals") || smstxt.contains("bellies") || smstxt.contains("heels") || smstxt.contains("pumps") || smstxt.contains("wedges") || smstxt.contains("toes") || smstxt.contains("stilettos")||smstxt.contains("gladiators")||smstxt.contains("lingerie")||smstxt.contains("camisole")||smstxt.contains("gown")||smstxt.contains("dupatta")||smstxt.contains("toe ring")||smstxt.contains("nose ring")) && (smstxt.contains("delivered") || smstxt.contains("ordered") || smstxt.contains("placed") || smstxt.contains("dispatched")|| smstxt.contains("order no.") || smstxt.contains("order") || smstxt.contains("shipped") || smstxt.contains("received")))
	    	    {
	        priority = 1.02;
	        attributeValue = "Yes";
	        
	             context.write(new Text(smsId+"-"+"gender"+"-"+"female"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "gender" + "\t" + "female" + "\t" +circle+ "\t" + operator  +  "\t" +fromNumber + "\t"+ Double.toString(priority) + "\t" +usercategory));
	             context.write(new Text(smsId+"-"+"freq_shopper"+"-"+"maybe"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "freq_shopper" + "\t" + "maybe" + "\t" +circle+ "\t" + operator  +"\t" +fromNumber + "\t"  + Double.toString(priority)+ "\t" +usercategory));

	             }
	     
	     if (attributeValue.length() > 0){
	      context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t"  +fromNumber + "\t" + Double.toString(priority) +"\t" +usercategory));
	    } 
	     } catch (InterruptedException e) {
	     e.printStackTrace();
	    }
		return  String.valueOf(attributeValue);
	   }


	   public String gadgets(String smstxt, String smsId, String phoneNo,String userid, String time, Context context,String operator,String circle,String fromNumber, String usercategory) throws IOException {

	    attributeName = "gadgets";
	    attributeValue = "";
	    if ((smstxt.contains("speaker") || smstxt.contains("bluetooth") ||smstxt.contains("xiomi")|| smstxt.contains("soundbar") || smstxt.contains("ipod") || smstxt.contains("mp3 player") || smstxt.contains("mp4 player") 
	    		|| smstxt.contains("video player") || smstxt.contains("media streaming") || smstxt.contains("device") || smstxt.contains("fm radio") || smstxt.contains("boom box") || smstxt.contains("video glasses") || smstxt.contains("remote controller")
	    		|| smstxt.contains("voltage stabilizers") || smstxt.contains("camera") || smstxt.contains("canon") || smstxt.contains("nikon") || smstxt.contains("sony") || smstxt.contains("go pro") || smstxt.contains("cell phone") || smstxt.contains("mobile phone") || smstxt.contains("tripod") 
	    		|| smstxt.contains("memory card") || smstxt.contains("binocular") || smstxt.contains("battery") || smstxt.contains("memory card") || smstxt.contains("gamepad") || smstxt.contains("headset") || smstxt.contains("headphone") || smstxt.contains("gaming mice") || smstxt.contains("intex") || smstxt.contains("lava")
	    		|| smstxt.contains("samsung") || smstxt.contains("micromax") || smstxt.contains("karbon") || smstxt.contains("adcom") || smstxt.contains("hitech") || smstxt.contains("i kall") || smstxt.contains("zopo") || smstxt.contains("airtyme") || smstxt.contains("akai") || smstxt.contains("alkatel") || smstxt.contains("apple") || smstxt.contains("arise") || smstxt.contains("asus") || smstxt.contains("blackberry") || smstxt.contains("bq mobile") 
	    		|| smstxt.contains("byond") || smstxt.contains("celkon") || smstxt.contains("datawind") || smstxt.contains("emerin") || smstxt.contains("formin") || smstxt.contains("gionee") || smstxt.contains("go hello") || smstxt.contains("hsl") || smstxt.contains("htc") || smstxt.contains("huawei") || smstxt.contains("iball") || smstxt.contains("ice x") || smstxt.contains("ismart") || smstxt.contains("geotex") || smstxt.contains("kenxinda") || smstxt.contains("kestrel") || smstxt.contains("lenovo") || smstxt.contains("lg") || smstxt.contains("maxx") || smstxt.contains("microsoft") || smstxt.contains("mitashi") || smstxt.contains("motorola") 
	    		|| smstxt.contains("nexg") || smstxt.contains("nokia") || smstxt.contains("onida") || smstxt.contains("oppo") || smstxt.contains("panasonic") || smstxt.contains("philip")  || smstxt.contains("salora") || smstxt.contains("Saral Sigmatel") || smstxt.contains("smartplay") || smstxt.contains("sony") || smstxt.contains("spice") || smstxt.contains("subway") || smstxt.contains("swipe") || smstxt.contains("tmax") || smstxt.contains("trio mobile") || smstxt.contains("vbera") || smstxt.contains("videocon") || smstxt.contains("vinner") || smstxt.contains("vox") || smstxt.contains("wham") || smstxt.contains("wynncom") || smstxt.contains("xccess") || smstxt.contains("xelectron") || smstxt.contains("xillion") || smstxt.contains("zen") || smstxt.contains("zte") || smstxt.contains("zync") || smstxt.contains("tablet") || smstxt.contains("power bank") || smstxt.contains("batteries") || smstxt.contains("charger") || smstxt.contains("data cable") || smstxt.contains("memory card") || smstxt.contains("video game") || smstxt.contains("desktop") || smstxt.contains("mouse") || smstxt.contains("keyboard") || smstxt.contains("monitor") || smstxt.contains("webcam") || smstxt.contains("projector") || smstxt.contains("printer") || smstxt.contains("cables") || smstxt.contains("router") || smstxt.contains("smart watch") || smstxt.contains("data card") 
	    		|| smstxt.contains("television") || smstxt.contains("trimmer") ||smstxt.contains("croma")|| smstxt.contains("philips") || smstxt.contains("hair dryers")) || smstxt.contains("big cinemas") && (smstxt.contains("delivered") || smstxt.contains("ordered") || smstxt.contains("placed") || smstxt.contains("dispatched")|| smstxt.contains("order no.") || smstxt.contains("order") || smstxt.contains("shipped") || smstxt.contains("received"))) {
	     priority = 1.01;
	     attributeValue = "Yes";
	    }
	    try {
	    	
	     if (attributeValue.length() > 0){
	      context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t" +fromNumber + "\t" + Double.toString(priority) +"\t" +usercategory));
	    } 
	     }catch (InterruptedException e) {
	     e.printStackTrace();
	    }
		return  String.valueOf(attributeValue);
	    
	   }

	   public String sports(String smstxt, String smsId, String phoneNo,String userid, String time, Context context,String operator,String circle,String fromNumber, String usercategory) throws IOException {
	    attributeName = "sports";
	    attributeValue = "";
	    if ((smstxt.contains("adidas") || smstxt.contains("sparx") || smstxt.contains("puma") || smstxt.contains(" hrx") || smstxt.contains(" nike") || smstxt.contains("reebok") || smstxt.contains(" yonex") || smstxt.contains(" nivia") || smstxt.contains("headly") || smstxt.contains("cricket") || smstxt.contains("football") || smstxt.contains("badminton") || smstxt.contains("tennis") || smstxt.contains("swimming") || smstxt.contains("basketball") || smstxt.contains("boxing") || smstxt.contains("cycling") || smstxt.contains("camping") || smstxt.contains("hiking") || smstxt.contains("skating") || smstxt.contains("hockey") || smstxt.contains("volley ball") || smstxt.contains("squash") || smstxt.contains("golf") || smstxt.contains("billiad") || smstxt.contains("pool")) && (smstxt.contains("delivered") || smstxt.contains("ordered") || smstxt.contains("placed") || smstxt.contains("dispatched") || smstxt.contains("order no.") || smstxt.contains("order") || smstxt.contains("shipped") || smstxt.contains("received"))) {
	     priority = 1.01;
	     attributeValue = "Yes";
	    }
	    try {
	    	
	        if (attributeValue.length() > 0){
	         context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t"  +circle+ "\t" + operator  +"\t"  +fromNumber + "\t"+ Double.toString(priority) + "\t" +usercategory));
	       } 
	       }catch (InterruptedException e) {
	        e.printStackTrace();
	       }
		return  String.valueOf(attributeValue);
	      }
	   public String brand_shopper(String smstxt, String smsId, String phoneNo,String userid, String time, Context context,String operator,String circle,String fromNumber, String usercategory) throws IOException {
		    
		    attributeValue = "";
		    if (smstxt.contains("tiffany & co.") || smstxt.contains("prada") || smstxt.contains("dolce & gabbana") 
		    		|| smstxt.contains("gucci") || smstxt.contains("armani") || smstxt.contains("chanel") || smstxt.contains("louis vuitton") 
		    		|| smstxt.contains("anita dongre") || smstxt.contains("bottega veneta") || smstxt.contains("burberry") 
		    		|| smstxt.contains("bvlgari") || smstxt.contains("cartier") || smstxt.contains("dior")
		    		|| smstxt.contains("dkny") || smstxt.contains("jimmy choo") || smstxt.contains("michael kors")
		    		|| smstxt.contains("porsche") || smstxt.contains("raghavendra rathore") || smstxt.contains("roberto cavalli") 
		    		|| smstxt.contains("tarun tahiliani") || smstxt.contains("tom ford") || smstxt.contains("varun bahl") 
		    		|| smstxt.contains("rina dhaka") || smstxt.contains("ritu kumar") || smstxt.contains("salvatore ferragamo") 
		    		|| smstxt.contains("versace") || smstxt.contains("villeroy & boch") || smstxt.contains("bugatti") || smstxt.contains("clarks")
		    		|| smstxt.contains("skechers") || smstxt.contains("steve madden") || smstxt.contains("calvin klein")) {
		     priority = 1.01;
		     attributeValue = "Yes";
		     attributeName = "premium_brand";
		    }
		    else if (smstxt.contains("diesel") || smstxt.contains("satya paul") || smstxt.contains("adidas") 
		    		|| smstxt.contains("aldo") || smstxt.contains("allen aolly") || smstxt.contains("american tourister") || smstxt.contains("andrew hill") 
		    		|| smstxt.contains("arrow") || smstxt.contains("biba") || smstxt.contains("bombay dyeing") 
		    		|| smstxt.contains("converse") || smstxt.contains("catwalk") || smstxt.contains("dorothy perkins")
		    		|| smstxt.contains("fasttrack") || smstxt.contains("french connection") || smstxt.contains("giordano")
		    		|| smstxt.contains("hidesign") || smstxt.contains("jack & jones") || smstxt.contains("hush puppies") 
		    		|| smstxt.contains("jockey") || smstxt.contains("lacoste") || smstxt.contains("lakme") 
		    		|| smstxt.contains("lee cooper") || smstxt.contains("louis philippe") || smstxt.contains("levi's") 
		    		|| smstxt.contains("mango") || smstxt.contains("maxima") || smstxt.contains("maybelline") || smstxt.contains("metro")
		    		|| smstxt.contains("nike") || smstxt.contains("numero uno") || smstxt.contains("only")
		    		|| smstxt.contains("pepe jeans") || smstxt.contains("peter england") || smstxt.contains("playboy") 
		    		|| smstxt.contains("puma") || smstxt.contains("raymond") || smstxt.contains("red chief") 
		    		|| smstxt.contains("red tape") || smstxt.contains("reebok") || smstxt.contains("timberlannd")
		    		|| smstxt.contains("titan") || smstxt.contains("tommy hilfiger") || smstxt.contains("top shop") 
		    		|| smstxt.contains("van heusen") || smstxt.contains("vero moda") || smstxt.contains("woodland")) {
		     priority = 1.01;
		     attributeValue = "Yes";
		     attributeName = "avg_brand";
		    } 
		    try {
		    	
		        if (attributeValue.length() > 0){
		         context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t"  +fromNumber + "\t" + Double.toString(priority) + "\t" +usercategory));
		       } 
		        }catch (InterruptedException e) {
		        e.printStackTrace();
		       }
			return  String.valueOf(attributeValue);
		      }

	   

	   public String movies(String smstxt, String smsId, String phoneNo,String userid, String time, String fromNumber, Context context,String operator,String circle, String usercategory) throws IOException {

	    attributeName = "movies";
	    attributeValue = "";
	    if(smstxt.contains("movie") && (smstxt.contains("booked") || smstxt.contains("confirmed") || smstxt.contains("booking no")))
	    {
	    	 priority = 1.01;
	         attributeValue = "Yes";
	    }
	    else if (fromNumber == "BMSHOW") {
	     priority = 1.02;
	     attributeValue = "Yes";
	    }
	    try {
	    	
	     if (attributeValue.length() > 0)
	     {
	      context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t"   +fromNumber + "\t"+ Double.toString(priority) + "\t" +usercategory));

	     
	     context.write(new Text(smsId+"-"+"savings_account"+"-"+"Yes"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "savings_account" + "\t" + "Yes" + "\t" +circle+ "\t" + operator  + "\t"  +fromNumber + "\t" + Double.toString(priority)+ "\t" +usercategory));
	     context.write(new Text(smsId+"-"+"net_banking"+"-"+"Yes"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "net_banking" + "\t" + "Yes" + "\t"  +circle+ "\t" + operator  +   "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));

	     
	     } 
	    } catch (InterruptedException e) {
	     e.printStackTrace();
	    }
		return  String.valueOf(attributeValue);
	   }

	   //todo need to review and add more rules
	   public String frequent_traveller(String smstxt, String smsId, String phoneNo,String userid, String time, String fromNumber, Context context,String operator,String circle, String usercategory) throws IOException {
	    
	    attributeName = "freq_traveller";
	    attributeValue = "maybe";  
	    String a="";
	    try {
	    if (smstxt.contains("flight") && (smstxt.contains("confirmation") || smstxt.contains(" pnr") || smstxt.contains("confirm")))
	    {a=attributeValue ;
	    	priority = 1.01;
	    	 
	        context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t"  +circle+ "\t" + operator  +"\t" +fromNumber + "\t" + Double.toString(priority) + "\t" +usercategory));
	    }                             
	    else if ((fromNumber.equals("GoIBIB") && smstxt.contains("check-in gates"))) {
	    	a=attributeValue ;
	    	priority = 1.02;
	        context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue +"\t" +circle+ "\t" + operator  +   "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
	       }
	    else if ((fromNumber.equals("IndiGo") && smstxt.contains("from gate"))) {
	    	a=attributeValue ;
	    	priority = 1.03;
	        context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue +"\t" +circle+ "\t" + operator  +   "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
	       }
	    else if (fromNumber == "ONEWAY" && smstxt.contains(" from") && smstxt.contains("airport") && smstxt.contains("confirmed")) {
	    	a=attributeValue ;
	    	priority = 1.04;
	    
	         context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t"  +circle+ "\t" + operator  +"\t" +fromNumber + "\t" + Double.toString(priority) + "\t" +usercategory));
	       }
	       else if (fromNumber == "GOLTRP" && smstxt.contains("flight") && smstxt.contains("confirmed") ) {
	    	   a=attributeValue ;
	    	   priority = 1.04;
	        
	         context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue +"\t" +circle+ "\t" + operator  +"\t"   +fromNumber + "\t" + Double.toString(priority)+ "\t" +usercategory));
	       }
	       else if (fromNumber == "KESARI" && smstxt.contains("confirmed") ) {
	    	   a=attributeValue ;
	    	   priority = 1.04;
	         
	         context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue +"\t" +circle+ "\t" + operator  +"\t"   +fromNumber + "\t" + Double.toString(priority)+ "\t" +usercategory));
	       
	       }
	       else if (fromNumber == "KKTRVL" && smstxt.contains("booking")  && smstxt.contains("confirmed") && smstxt.contains("flight")) {
	    	   a=attributeValue ;
	    	   priority = 1.04;
	          context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t"  +circle+ "\t" + operator  +"\t"  +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
	        }

	       else if (fromNumber == "FullOn" && smstxt.contains("passenger") && smstxt.contains("terminal") && smstxt.contains("confirmed")) {
	    	   a=attributeValue ;
	    	   priority = 1.04;
	         
	         context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +  "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
	        }

	       else if (fromNumber == "BUSTVL" && smstxt.contains("pnr") && smstxt.contains("departing") && smstxt.contains("flight")) {
	    	   a=attributeValue ;
	    	   priority = 1.04;
	        context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue +"\t" +circle+ "\t" + operator  +   "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
	       } // to do
	    
	    } catch (InterruptedException e) {

	        e.printStackTrace();
	       }
		return a;
	   }

	  
	   public String frequent_shopper(String smstxt, String smsId, String phoneNo,String userid, String time, String fromNumber, Context context,String operator,String circle, String usercategory) throws IOException {
		   String a="" ;
	    String t = time;
	    attributeName = "freq_shopper";
	    attributeValue = "maybe";
	    try {
	    
	    if(smstxt.contains("ordered") || smstxt.contains("delivered") || smstxt.contains("order placed") || (smstxt.contains("product") && smstxt.contains("order no")))
	    {	   a=attributeValue ;
	    	priority = 1.01;
	    	context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo+"\t"+ userid+ "\t" + t + "\t" + attributeName + "\t" + attributeValue +"\t" +circle+ "\t" + operator  +"\t"  +fromNumber + "\t" + Double.toString(priority)+ "\t" +usercategory));
	    }
	    else if (smstxt.contains("paytm wallet ") ||smstxt.contains("dear oxigen" ) ){ 
	    	priority = 1.01;
	 	   a=attributeValue ;
	            context.write(new Text(smsId+"-"+"freq_shopper"+"-"+"maybe"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "freq_shopper" + "\t" + "maybe" + "\t" +circle+ "\t" + operator  +"\t"  +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
	            context.write(new Text(smsId+"-"+"savings_account"+"-"+"Yes"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "savings_account" + "\t" + "Yes" + "\t" +circle+ "\t" + operator  + "\t"   +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
	            context.write(new Text(smsId+"-"+"net_banking"+"-"+"Yes"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "net_banking" + "\t" + "Yes" + "\t"  +circle+ "\t" + operator  +   "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));

	       }
	    else if ((fromNumber == "Dotzot"||fromNumber == "FARMSB" ||fromNumber == "REEBOK"||fromNumber == "HBUDDY"|| fromNumber == "FabFur" ||fromNumber == "ADIDAS"|| 
	    		fromNumber == "56070" || fromNumber == "HPSTCH" || fromNumber == "MSSDCL" || fromNumber == "PLANTM" || fromNumber == "HONEST"
	    		|| fromNumber == "BUCKET" || fromNumber == "zobelo" || fromNumber == "eshops" || fromNumber == "GIFTEZ" 
	    		|| fromNumber == "JUPTSH" || fromNumber == "FABONE" || fromNumber == "MFTREE" || fromNumber == "FPANDA" || fromNumber == "SNAPDEAL" 
	    				 || fromNumber == "SWIGGY"  || fromNumber == "SUBWAY" || fromNumber == "ZOPBZR"|| fromNumber == "RedExp"|| fromNumber == "Myntra"|| fromNumber == "TRNDIN")&& smstxt.contains("ordered") && smstxt.contains("delivered")) 
	    {	   a=attributeValue ;
	     priority = 1.01; // to do 
	       context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid + "\t" + t + "\t" + attributeName + "\t" + attributeValue +"\t" +circle+ "\t" + operator  +"\t"  +fromNumber + "\t" + Double.toString(priority)+ "\t" +usercategory));
	    }
	    else if(smstxt.contains("paytm ") && (smstxt.contains(" rs") || smstxt.contains(" inr")))
	    {	   a=attributeValue ;
	    	priority = 2.01;
	    	context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+"\t" + t + "\t" + attributeName + "\t" + attributeValue +"\t" +circle+ "\t" + operator  +"\t"  +fromNumber + "\t" + Double.toString(priority)+ "\t" +usercategory));
	    }
	    
	    } catch (InterruptedException e) {

	        e.printStackTrace();
	       }
		return a;
	   }


	   public String health_insurance(String smstxt, String smsId, String phoneNo,String userid, String time, String fromNumber,String usercategory, Context context,String operator,String circle) throws IOException {
	    attributeName = "health_insurance";
	    attributeValue = "";
	   
	    try {
	    if (  (smstxt.contains("health insurance") || smstxt.contains("medical insurance") || smstxt.contains("health policy") || smstxt.contains("lombard policy") )  && (  smstxt.contains("rs.") || smstxt.contains("inr") || smstxt.contains("applying") || smstxt.contains("confirmed") || smstxt.contains("rs") || smstxt.contains("application in process") || smstxt.contains("due on ")  )      ) 
	    	{    
	    	priority = 1.01;
	    	     attributeValue = "Yes";
	    	}
	    else if ( userid.equals("infosysbpopun")||userid.equals("infosyschaa") ||fromNumber.equals("Wipro") || fromNumber.equals("Oracle") ) { ///hardcoded
	        priority = 1.02;
	        attributeValue = "Yes";
	        context.write(new Text(smsId+"-"+"savings_account"+"-"+"Yes"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "savings_account" + "\t" + "Yes" + "\t" +circle+ "\t" + operator  + "\t"   +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
	        context.write(new Text(smsId+"-"+"net_banking"+"-"+"Yes"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "net_banking" + "\t" + "Yes" + "\t"  +circle+ "\t" + operator  +   "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));

	       }

	    else if (fromNumber.equals("INFIBL") || userid.equals("religare") || fromNumber.equals("SUDLIF")) {  ////hardcoded
	           priority = 2.01;
	           attributeValue = "Yes";
	          }
	    
	     if (attributeValue.length() > 0){
	      context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue +"\t" +circle+ "\t" + operator  +   "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
	    }
	     }catch (InterruptedException e) {
	     e.printStackTrace();
	    }
		return  String.valueOf(attributeValue);
	   }

	   public String mutual_fund(String smstxt, String smsId, String phoneNo,String userid, String time, Context context,String operator,String circle,String usercategory,String fromNumber) throws IOException {

	    attributeName = "mutual_fund";
	    attributeValue = "";
	   

	    if (smstxt.contains("mutual fund") || smstxt.contains("bseindia") || smstxt.contains("sharekhan") || smstxt.contains(" folio") && !(smstxt.contains("thankyou"))) {
	     priority = 1.01;
	     attributeValue = "Yes";
	    }
	    else if ((smstxt.contains("account") || smstxt.contains(" a/c")  || smstxt.contains("scheme")) && smstxt.contains("balance") && fromNumber.equals("RMFund") ) {
	        priority = 1.02;
	        attributeValue = "Yes";
	       }
	    
	    else if ((smstxt.contains(" stp") && smstxt.contains(" folio")) || smstxt.contains("dividend") ) {
	     priority = 2.01;
	     attributeValue = "Yes";
	    }
	  
	    try {
	    	
	     if (attributeValue.length() > 0) {
	      context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +"\t"+fromNumber + "\t" + Double.toString(priority)+ "\t" +usercategory));
	      
	      context.write(new Text(smsId+"-"+"savings_account"+"-"+"Yes"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "savings_account" + "\t" + "Yes" + "\t" +circle+ "\t" + operator  + "\t"  +fromNumber + "\t" + Double.toString(priority)+ "\t" +usercategory));
	      context.write(new Text(smsId+"-"+"net_banking"+"-"+"Yes"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "net_banking" + "\t" + "Yes" + "\t"  +circle+ "\t" + operator  +   "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
	     }	
	    } catch (InterruptedException e) {
	     e.printStackTrace();
	    }
		return  String.valueOf(attributeValue);

	   }

	   public String home_loan(String smstxt, String smsId, String phoneNo,String userid, String time, String fromNumber, Context context,String operator,String circle, String usercategory) throws IOException {

	    attributeName = "home_loan";
	    attributeValue = "";
	    try {
	    if ((smstxt.contains("home loan") || (smstxt.contains("home") && smstxt.contains("loan"))) && (smstxt.contains(" rs") || smstxt.contains(" inr") || smstxt.contains("due by") || smstxt.contains("due on")) ) //home loan
	    {
	     priority = 1.01;
	     attributeValue = "Yes";
	    }
	    else if (smstxt.contains("home") && smstxt.contains("loan") && (smstxt.contains("applying") || smstxt.contains("application") ) ) //home loan
	    {
	     priority = 1.02;
	     attributeValue = "Yes";
	    }
	    
	     if (attributeValue.length() > 0) {
	      context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t"  +circle+ "\t" + operator  +"\t"   +fromNumber + "\t" + Double.toString(priority)+ "\t" +usercategory));
	      context.write(new Text(smsId+"-"+"savings_account"+"-"+"Yes"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "savings_account" + "\t" + "Yes" + "\t" +circle+ "\t" + operator  + "\t"   +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
	      context.write(new Text(smsId+"-"+"net_banking"+"-"+"Yes"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "net_banking" + "\t" + "Yes" + "\t"  +circle+ "\t" + operator  +   "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
	      context.write(new Text(smsId+"-"+"life_insurance"+"-"+"Yes"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "life_insurance" + "\t" + "Yes" + "\t" +circle+ "\t" + operator     + "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
	     }
	    } catch (InterruptedException e) {
	     e.printStackTrace();
	    }
		return  String.valueOf(attributeValue);
	   }

	   public String internet_banking(String smstxt, String smsId, String phoneNo,String userid, String time, Context context,String operator,String circle,String usercategory,String fromNumber) throws IOException {
	    attributeName = "net_banking";
	    attributeValue = "";
	   
	    if ((smstxt.contains("internet banking") || smstxt.contains("inter net bkg.") || smstxt.contains("netbanking") || smstxt.contains("e_banking")) && (smstxt.contains("beneficiary") || smstxt.contains("transaction") || smstxt.contains("tranx") || smstxt.contains("subscribing") || smstxt.contains("demat") || smstxt.contains("open"))) {
	     priority = 1.01;
	     attributeValue = "Yes";
	    }
	    else if(smstxt.contains("mutual fund") || smstxt.contains("capital market") || smstxt.contains("sharekhan") || smstxt.contains("sip") && smstxt.contains("rs"))
	    {
	    	 priority = 1.02;
	         attributeValue = "Yes";
	    	
	    }
	         else if (smstxt.contains("paytm wallet") || smstxt.contains("paytm")  ) {
	     priority = 1.03;
	     attributeValue = "Yes";
	    }
	    else if (smstxt.contains(" neft") &&( smstxt.contains("debit") || smstxt.contains("credit"))  ) {
	             priority = 1.04;
	             attributeValue = "Yes";
	    }  
	    else if (smstxt.contains(" otp") &&( smstxt.contains("net banking") || smstxt.contains("internet banking"))  ) {
	     priority = 1.05;
	     attributeValue = "Yes";
	    }
	        
	    else if ((smstxt.contains("mobile banking") || smstxt.contains("banking")) && (smstxt.contains(" mpin") || smstxt.contains(" otp")) ) {
	     priority = 1.06;
	     attributeValue = "Yes";
	    }
	      else if (smstxt.contains("redeem") && smstxt.contains("points") ) {
	     priority = 1.07;
	     attributeValue = "Yes";
	    }
	     
	      else if (smstxt.contains(" imps") || smstxt.contains(" upi") || smstxt.contains(" rtgs") ) {
	     priority = 1.08;
	     attributeValue = "Yes";
	    }

	     else if (smstxt.contains("fino money") || smstxt.contains("mwallet") || smstxt.contains("m wallet") ) {
	     priority = 1.09;
	     attributeValue = "Yes";
	    }
	     else if (fromNumber.contains("WIPRO") || fromNumber.contains("ORACLE")){
	    	 priority = 2.01;
	         attributeValue = "Yes";   	 
	     }
	     else if (smstxt.contains("gocash") && fromNumber.equals("GOIBIBO") ) {
	     priority = 2.02;
	     attributeValue = "Yes";
	    }
	    try {
	    	
	     if (attributeValue.length() > 0){
	      context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +   "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
	    }
	     }catch (InterruptedException e) {
	     e.printStackTrace();
	    }
		return  String.valueOf(attributeValue);
	   }
	   
	   public String creditcard_limit(String smstxt, String smsId, String phoneNo,String userid, String time, Context context,String operator,String circle,String usercategory,String fromNumber) throws IOException {
		    attributeName = "creditcard_limit";
		    attributeValue = "";
		    priority = 1.01;
		    if (fromNumber == "RBLBNK"){
		        
	            String s = smstxt.substring(smstxt.indexOf("rs.")+4, smstxt.indexOf("rs.")+11);
	            
	            String ss= s.replaceAll("[^\\d.]", "");            
	            attributeValue=ss;                    }
		    else if (fromNumber == "ICICIB" && smstxt.contains("total cr lmt:")){
	            String s = smstxt.substring(smstxt.indexOf("total cr lmt:")+18, smstxt.indexOf("total cr lmt:")+29);
	            
	            String ss= s.replaceAll("[^\\d.]", "");
	            attributeValue=ss;          }
	        else if (fromNumber == "RATNAK"){
	            String s = smstxt.substring(smstxt.indexOf("exceeded the limit")+19, smstxt.indexOf("exceeded the limit")+28);
	            
	            String ss= s.replaceAll("[^\\d.]", "");
	            attributeValue=ss;                                }
	        else if (fromNumber == "SBICRD"){
	            String s = smstxt.substring(smstxt.indexOf("available limit is ")+22, smstxt.indexOf("available limit is")+33);
	            
	            String ss= s.replaceAll("[^\\d.]", "");
	            attributeValue=ss;                                }
	        else if (fromNumber == "Citibank"){
	       
	        	
	            String s = smstxt.substring(smstxt.indexOf("available limit is ")+19, smstxt.indexOf("available limit is")+27);
	            
	            String ss= s.replaceAll("[^\\d.]", "");
	            attributeValue=ss;
	                                            }
	        else if (fromNumber == "TATACD"){
	       
	            String s = smstxt.substring(smstxt.indexOf("available limit is ")+22, smstxt.indexOf("available limit is")+31);
	            
	            String ss= s.replaceAll("[^\\d.]", "");
	            attributeValue=ss;                                }

	        else if (smstxt.contains("credit card") && smstxt.contains("limit") && smstxt.contains("changed from") ){
	            if (smstxt.contains(" inr")){
	         	   smstxt=smstxt.replace(".", " ");
	         	   String s1 = smstxt.substring(smstxt.indexOf(" inr"),smstxt.length());
	         	
	         	   s1 = s1.substring(s1.indexOf(" to"),s1.length());
	            
	         	   s1 = s1.substring(s1.indexOf("inr"),s1.length());
	         	
	         	   String smslist[] = new String[20];
	         	   smslist = s1.split(" ");
	         	   
	         	
	    		 String	 ss=smslist[1];
	    	

	    				 ss= ss.replaceAll("[^\\d.]", "");
	             
	             attributeValue=ss;
	             }
	            else if (smstxt.contains(" rs")){
	         	   smstxt=smstxt.replace(".", " ");
	         	   String s1 = smstxt.substring(smstxt.indexOf(" rs"),smstxt.length());
	         	   s1 = s1.substring(s1.indexOf(" to"),s1.length());
	         	   s1 = s1.substring(s1.indexOf("rs"),s1.length());
	         	   String smslist[] = new String[20];
	    				 smslist = s1.split(" ");
	    				 String	 ss=smslist[1];
	              ss= ss.replaceAll("[^\\d.]", "");
	             
	             attributeValue=ss;
	             }
	            }
	        else if (smstxt.contains("card with limit")){

	            String s = smstxt.substring(smstxt.indexOf("rs.")+4, smstxt.indexOf("rs.")+11);
	            
	            String ss= s.replaceAll("[^\\d.]", "");
	            ss= ss.replaceAll(". ", "");
	            attributeValue=ss;
	           }
	        else if ((smstxt.contains("credit card") || smstxt.contains(" cc") || smstxt.contains("card")) && smstxt.contains("limit") && smstxt.contains("changed from"))
		    {
	        	priority = 1.01;
	  		  String s = smstxt.substring(smstxt.indexOf("rs")+10,smstxt.length());
	  		   String ss= s.replaceAll("[^\\d.]", "");
	              ss= ss.replaceAll(". ", "");
	              attributeValue = ss;
		    }
	        else if ((smstxt.contains("credit card limit") || smstxt.contains("credit limit")) && (smstxt.contains(" rs") || smstxt.contains(" inr") || smstxt.contains(" rs.")))
		    {
			  priority = 1.02;
			  String s = smstxt.substring(smstxt.indexOf("rs")+10,smstxt.length());
			   String ss= s.replaceAll("[^\\d.]", "");
	            ss= ss.replaceAll(". ", "");
	            attributeValue = ss;
		    }
	        else if ((smstxt.contains("credit card limit") || smstxt.contains("credit limit") || smstxt.contains("cc limit")) && (smstxt.contains(" rs") || smstxt.contains(" inr") || smstxt.contains(" rs.")))
		    {
			  priority = 1.02;
			  String s = smstxt.substring(smstxt.indexOf("rs")+100,smstxt.length());
			   String ss= s.replaceAll("[^\\d.]", "");
	            ss= ss.replaceAll(". ", "");
	            attributeValue = ss;
		    }
		    try {
		    	
		     if (attributeValue.length() > 0){
		      context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" +circle+ "\t" + operator  +   "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
		    } 
		     }catch (InterruptedException e) {
		     e.printStackTrace();
		    }
			return  String.valueOf(attributeValue);
		   }
	   public String dob (String smstxt, String smsId, String phoneNo,String userid, String time, Context context,String fromNumber,String operator,String circle, String usercategory) throws IOException {

		    attributeName = "dob";
		    attributeValue = "";
		    if (smstxt.contains("dob :") ) {
		     priority = 1.01;
		     attributeValue = "Yes";
		     String s = smstxt.substring(smstxt.indexOf("dob :"),smstxt.indexOf("dob :")+19 );
		     
		     if (s.contains("/")){

		           s=s.substring(s.indexOf("dob :")+6,s.indexOf("dob :")+16);
		           s= s.replaceAll("[^\\d.]", "");
		                              }
		     else{
		     	 s=s.substring(s.indexOf("dob :")+6,s.indexOf("dob :")+16);
		     	s= s.replaceAll("[^\\d.]", "");
		     
		            }
		     attributeValue =s;
		    }
		    try {
		    	
		     if (attributeValue.length() > 0)
		     {
		      context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t"+circle+ "\t" + operator  +"\t"  +fromNumber + "\t" + Double.toString(priority) + "\t" +usercategory));
		      }
		    } catch (InterruptedException e) {
		     e.printStackTrace();
		    }
			return  String.valueOf(attributeValue);
		   }

	   public String age(String smstxt, String smsId, String phoneNo,String userid, String time, Context context,String fromNumber,String operator,String circle, String usercategory) throws IOException {

	    attributeName = "age";
	    attributeValue = "";
	 try {
		 
		 if ((smstxt.contains("dear student") || smstxt.contains("dear students")) && (smstxt.contains("iit ") || smstxt.contains("jee books ")   ||  smstxt.contains("ssc board exam") ||smstxt.contains(" aipmt ") ||smstxt.contains("clat ") ||   smstxt.contains("board examination ") || smstxt.contains("10th class ") || smstxt.contains("12th class"))) {
	     priority = 1.01;
	     attributeValue = "<18";

	      context.write(new Text(smsId+"-"+"credit_card"+"-"+"no"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "credit_card" + "\t" + "no" + "\t" + circle+ "\t" + operator  +"\t" +fromNumber + "\t" +Double.toString(priority)+ "\t" +usercategory));
	      context.write(new Text(smsId+"-"+"income"+"-"+"0-3L"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "income" + "\t" + "0-3L" + "\t" + circle+ "\t" + operator  +"\t" +fromNumber + "\t" +Double.toString(priority)+ "\t" +usercategory));
	      context.write(new Text(smsId+"-"+"health_insurance"+"-"+"no"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "health_insurance" + "\t" + "no"+ "\t"+ circle+ "\t" + operator  +"\t"+fromNumber + "\t" + Double.toString(priority)+ "\t" +usercategory));
	      context.write(new Text(smsId+"-"+"has_kids"+"-"+"no"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "has_kids" + "\t" + "no" +"\t"+circle+ "\t" + operator  +"\t"+fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
	      context.write(new Text(smsId+"-"+"car_insurance"+"-"+"no"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "car_insurance" + "\t" + "no" + "\t"+circle+ "\t" + operator +"\t" +fromNumber + "\t" + Double.toString(priority)+ "\t" +usercategory));
	      context.write(new Text(smsId+"-"+"mutual_fund"+"-"+"no"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "mutual_fund" + "\t" + "no" + "\t"+circle+ "\t" + operator  +"\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
	      context.write(new Text(smsId+"-"+"car_loan"+"-"+"no"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "car_loan" + "\t" + "no" + "\t"+circle+ "\t" + operator   + "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
	      context.write(new Text(smsId+"-"+"home_loan"+"-"+"no"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "home_loan" + "\t" + "no"+"\t"+circle+ "\t" + operator   + "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));

	    }
		 else if (     (smstxt.contains("btech college") ||smstxt.contains("btech colleges ")||smstxt.contains("b.tech college ")||smstxt.contains("b.tech colleges "))
				 &&    ( smstxt.contains("admissions ") ||smstxt.contains("admission ")   )   )
		    {
				 priority = 1.03;
			     attributeValue = "18-25";
			     
		    }
		 else if ((smstxt.contains("fresher's ") || smstxt.contains("freshers ") || smstxt.contains("fresher ")) && (smstxt.contains("hiring ")|| smstxt.contains("hiring for ")|| smstxt.contains("mba/pgdm (aspirant)")))
	    {
			 priority = 1.03;
		     attributeValue = "18-25";
		     
	    }
		 	else if (userid.equals("PUOMBA")||userid.equals("Shksha")||userid.equals("CoCubes")||         ////hardcoded on senderids
		    		userid.equals("Intuittemp")||userid.equals("niityuvajltd")){
		 	     priority = 1.03;
			      
		 	   attributeValue="18-25";    	
		 	  context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "income"+ "\t" + "0-3L"+ "\t"+ circle+ "\t" + operator  +"\t"  +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
		    }
		    else if ((smstxt.contains("prepare ") && smstxt.contains(" mba ")) ||smstxt.contains("final yr")||smstxt.contains("final year")|| smstxt.contains("dear mba/pgdm aspirant") || smstxt.contains("freshers ")  || smstxt.contains("cat prep") || smstxt.contains("crack cat") || smstxt.contains("cat exam") || (smstxt.contains("admission open") && (smstxt.contains("diploma ") || smstxt.contains("college ")))) {
		     priority = 1.03;
		     attributeValue = "18-25";
		   
		       context.write(new Text(smsId+"-"+"health_insurance"+"-"+"no"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "health_insurance" + "\t" + "no"+ "\t"+ circle+ "\t" + operator  +"\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
		       context.write(new Text(smsId+"-"+"has_kids"+"-"+"no"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "has_kids" + "\t" + "no" +"\t"+circle+ "\t" + operator  +"\t"+fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
		       context.write(new Text(smsId+"-"+"car_insurance"+"-"+"no"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "car_insurance" + "\t" + "no" + "\t"+circle+ "\t" + operator +"\t"  +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
		       context.write(new Text(smsId+"-"+"mutual_fund"+"-"+"no"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "mutual_fund" + "\t" + "no" + "\t"+circle+ "\t" + operator  +"\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
		       context.write(new Text(smsId+"-"+"car_loan"+"-"+"no"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "car_loan" + "\t" + "no" + "\t"+circle+ "\t" + operator   + "\t"+fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
		       context.write(new Text(smsId+"-"+"home_loan"+"-"+"no"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "home_loan" + "\t" + "no"+"\t"+circle+ "\t" + operator   + "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
		      
		    
		    } 
		    else if ((smstxt.contains("hiring ") && smstxt.contains("fresher ")) ||smstxt.contains("freshers ")||smstxt.contains("final year")|| smstxt.contains("dear mba/pgdm aspirant") || smstxt.contains("freshers ") || smstxt.contains("cat prep") || smstxt.contains("crack cat") || smstxt.contains("cat exam") ||  smstxt.contains("cat exam") ||      (smstxt.contains("admission open") && (smstxt.contains("diploma") || smstxt.contains("college") || smstxt.contains("btech college")))) {
		        priority = 1.03;
		        attributeValue = "18-25";
		       
		          context.write(new Text(smsId+"-"+"health_insurance"+"-"+"no"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "health_insurance" + "\t" + "no"+ "\t"+ circle+ "\t" + operator  +"\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
		          context.write(new Text(smsId+"-"+"has_kids"+"-"+"no"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "has_kids" + "\t" + "no" +"\t"+circle+ "\t" + operator  +"\t"+fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
		          context.write(new Text(smsId+"-"+"car_insurance"+"-"+"no"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "car_insurance" + "\t" + "no" + "\t"+circle+ "\t" + operator +"\t"  +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
		          context.write(new Text(smsId+"-"+"mutual_fund"+"-"+"no"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "mutual_fund" + "\t" + "no" + "\t"+circle+ "\t" + operator  +"\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
		          context.write(new Text(smsId+"-"+"car_loan"+"-"+"no"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "car_loan" + "\t" + "no" + "\t"+circle+ "\t" + operator   + "\t"+fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
		          context.write(new Text(smsId+"-"+"home_loan"+"-"+"no"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "home_loan" + "\t" + "no"+"\t"+circle+ "\t" + operator   + "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
		         
		       
		       } 
		    else if (smstxt.contains("pension ") && smstxt.contains("credited ")) {
		        priority = 1.04;
		        attributeValue = ">50";
		       
		         
		         context.write(new Text(smsId+"-"+"health_insurance"+"-"+"Yes"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "health_insurance" + "\t" + "Yes" +"\t"+circle+ "\t" + operator +  "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
		         context.write(new Text(smsId+"-"+"has_kids"+"-"+"Yes"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "has_kids" + "\t" + "Yes" +"\t"+circle+ "\t" + operator+   "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
		       }
		    else if (smstxt.contains("dob :")) { //todo need to add more rules
		        priority = 1.06;
		        attributeValue = " ";
		        int age= Year.now().getValue();
		        String s="";
		        s = smstxt.substring(smstxt.indexOf("dob :"),smstxt.indexOf("dob :")+16).replace("/","");

		        if (s.contains("/"))
		        {
		            s=s.substring(s.indexOf("dob :")+12,s.indexOf("dob :")+16);
		        }else
		        {
		        	 s=s.substring(s.indexOf("dob")+10,s.indexOf("dob :")+14);
		        }
		             
		        if(tryParseInt(s)){
		        	age=age-(Integer.parseInt(s));
		            if ((age>0) && (age<18))
		              attributeValue="<18";
		            else if ((age>18) && (age<25))
		              attributeValue="18-25";
		                    else if ((age>25) && (age<30))
		              attributeValue="25-30";
		            else if ((age>30) && (age<35))
		              attributeValue="30-35";
		                                 
		            else if ((age>35) && (age<40))
		              attributeValue="35-40";
		                                 
		          else if ((age>40) && (age<45))
		              attributeValue="40-45";
		                                 
		          else if ((age>45) && (age<50))
		              attributeValue="40-45";
		                                 
		          else if ((age>50))
		              attributeValue=">50";
		        
		           
		            if (attributeValue =="30-35" || (attributeValue == "35-40") || attributeValue =="40-45" || attributeValue ==">50")
		             {
		            	context.write(new Text(smsId+"-"+"has_kids"+"-"+"Yes"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "has_kids" + "\t" + "Yes" +"\t"+circle+ "\t" + operator  + "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
		             }
		          }
		    } 
	   else if (
	    		(smstxt.contains("payment") && smstxt.contains("college") && smstxt.contains("successful")) 
				
				){
	  	     priority = 1.07;
			      
	  	   attributeValue="18-25";
	  	   
	    	} 

		 
	else if (( smstxt.contains("pickup") || smstxt.contains("pick-up") && smstxt.contains("drop ") && smstxt.contains("supervisor") ) || ( smstxt.contains("customer") && (smstxt.contains("pick-up") 
	    				  || smstxt.contains("pickup")) && smstxt.contains("drop")) || smstxt.contains("dear driver")) 
				
				{
	  	     priority = 1.08;
			      
	  	   attributeValue="18+";
	    	}
		    
	else if (smstxt.contains("dear") &&  smstxt.contains("matrimony")) { //todo need to add more rules
	 priority =2.01 ;
	 attributeValue = "25-30";

	  context.write(new Text(smsId+"-"+"has_kids"+"-"+"no"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "has_kids" + "\t" + "no" + "\t"+circle+ "\t" + operator + "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
		}    
	else if (smstxt.contains("simply marry")|| smstxt.contains("simplymarry")) { //todo need to add more rules
	 priority =2.02 ;
	 attributeValue = "25-30";

	  
	  context.write(new Text(smsId+"-"+"has_kids"+"-"+"no"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "has_kids" + "\t" + "no" + "\t"+circle+ "\t" + operator + "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));

	} else if (smstxt.contains("child education plan")) {
	 priority = 2.03;
	 attributeValue = "40-45";
	 
	  context.write(new Text(smsId+"-"+"health_insurance"+"-"+"Yes"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "health_insurance" + "\t" + "Yes" +"\t"+circle+ "\t" + operator  + "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
	  context.write(new Text(smsId+"-"+"has_kids"+"-"+"Yes"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "has_kids" + "\t" + "Yes" +"\t"+circle+ "\t" + operator +  "\t"+fromNumber + "\t" + Double.toString(priority)+ "\t" +usercategory));
		}  
	else if (fromNumber.contains("HPSTCH") ) {
	    priority = 2.04;
	    attributeValue = "35-40";
	 
		} 
	else if ((smstxt.contains("dear parent")) || (smstxt.contains("dear sir") && smstxt.contains("ward ")) && fromNumber.contains("AAKASH")  ){
	  	     priority = 2.05;
			      
	  	   attributeValue="40-45";
	  	 context.write(new Text(smsId+"-"+"has_kids"+"-"+"Yes"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "has_kids" + "\t" + "Yes" +"\t"+circle+ "\t" + operator+   "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));    	
	} 
		
		 
	   else if ((smstxt.contains("payment") && smstxt.contains("school ") ))
	   {
	  	     priority = 3.02;
			      
	  	   attributeValue="30-50";
	    	}
		 
	else if (fromNumber.contains("BARCHD")){
	  	     priority = 3.03;
			      
	  	   attributeValue="35+";
	    	} 
	else if ((smstxt.contains("dr ") || smstxt.contains("dr. ")) && (smstxt.contains("appointment") && smstxt.contains("with patient")) && fromNumber.contains("practo")  ){
	  	     priority = 3.04;
			      
	  	   attributeValue="40-45";
	    	} 
	else if (( smstxt.contains("interested")  && smstxt.contains("in yours") && (smstxt.contains("audi ")  || smstxt.contains("bmw ") || smstxt.contains("mercedez") || smstxt.contains("jaguar"))))
	{
	  	     priority = 3.05;
			      
	  	   attributeValue="40-45";
	    	}  
	else if (( smstxt.contains("customer")  && smstxt.contains("contacted") && smstxt.contains("audi "))  || smstxt.contains("bmw ") || smstxt.contains("mercedez") || smstxt.contains("jaguar" )) 
				{
	  	     priority = 3.05;
			      
	  	   attributeValue="40-45";
	    	}   	   

	    else if (smstxt.contains("senior citizen")) {
	     priority = 4.01;
	     attributeValue = ">50";
	   
	     context.write(new Text(smsId+"-"+"health_insurance"+"-"+"Yes"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "health_insurance" + "\t" + "Yes" +"\t"+circle+ "\t" + operator +  "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
	      context.write(new Text(smsId+"-"+"has_kids"+"-"+"Yes"+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + "has_kids" + "\t" + "Yes" +"\t"+circle+ "\t" + operator+   "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
	    }
		 
		 if (attributeValue.length() > 0)
	    {
	    	context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue+"\t"+circle+ "\t" + operator   + "\t" +fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
	    }
	    } catch (InterruptedException e) {

	        e.printStackTrace();
	       }
	return  String.valueOf(attributeValue);
	   }
	   
	   boolean tryParseInt(String value) {  
		     try {  
		         Integer.parseInt(value);  
		         return true;  
		      } catch (NumberFormatException e) {  
		         return false;  
		      }  
		}
	   
	   public String ecom_expense(String smstxt, String smsId, String phoneNo,String userid, String time, String fromNumber, Context context,String operator,String circle, String usercategory) throws IOException {
	   attributeName = "ecom_expense";
	   priority = 1.01;
	   
	   String str1 = "";
	   
	   if ((fromNumber == "Dotzot"||fromNumber == "FARMSB" ||fromNumber == "REEBOK"||fromNumber == "HBUDDY"|| fromNumber == "FabFur" ||fromNumber == "ADIDAS"|| 
	   		fromNumber == "56070" || fromNumber == "HPSTCH" || fromNumber == "MSSDCL" || fromNumber == "PLANTM" || fromNumber == "HONEST"
	   		|| fromNumber == "BUCKET" || fromNumber == "zobelo" || fromNumber == "eshops" || fromNumber == "GIFTEZ" 
	   		|| fromNumber == "JUPTSH" || fromNumber == "FABONE" || fromNumber == "MFTREE" || fromNumber == "FPANDA" || fromNumber == "SNAPDEAL" 
	   		|| fromNumber == "ZOPBZR"|| fromNumber == "RedExp"|| fromNumber == "Myntra"|| fromNumber == "TRNDIN"|| smstxt.contains("myntra")) 
	   		&& smstxt.contains("rs.")) {
		   str1 = smstxt.substring(smstxt.indexOf("rs.") + 4, smstxt.indexOf("rs.") + 10);
		   
		 if (str1.contains(".")) {
		    str1=str1.substring(0,str1.indexOf(".") );}
		else if 
		     (str1.contains("/")) {
		    str1=str1.substring(0,str1.indexOf("/") );
		    }

		    try {
		    	public_attribute = str1;
		        context.write(new Text(smsId+"-"+attributeName+"-"+str1+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + str1+"\t"+circle+ "\t" + operator   + "\t"+fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
		        
		       } catch (InterruptedException e) {

		        e.printStackTrace();
		       }
		    
	   }
	   return  String.valueOf(str1);     
	         
	   }

	   public String withdrawl(String smstxt, String smsId, String phoneNo,String userid, String time, String fromNumber, Context context,String operator,String circle, String usercategory) throws IOException {
		 
			    attributeName = "withdrawl";
		    attributeValue = "";
		    priority = 1.01;
	    String t=time,str1="",str2="";

		    if (smstxt.contains("debited") ){
		 	   
			     if (smstxt.contains(" rs.")){

			    	    str1 = smstxt.substring(smstxt.indexOf("rs."), smstxt.indexOf("rs.") + 20);
			    	    str2= str1.substring(str1.indexOf("rs.")+3,str1.indexOf(" ",4));
	 	                attributeValue = str2.replaceAll("[^\\d.]", "");
	    	}
			    	else if (smstxt.contains("inr ")){
			       	 str1 = smstxt.substring(smstxt.indexOf("inr "), smstxt.indexOf("inr ") + 20);
			       	str2= str1.substring(str1.indexOf("inr ")+3,str1.indexOf(" ",4));
			        attributeValue = str2.replaceAll("[^\\d.]", "");
			    	}
				else if (smstxt.contains("inr")){
			       	 str1 = smstxt.substring(smstxt.indexOf("inr"), smstxt.indexOf("inr") + 20);
			       	str2= str1.substring(str1.indexOf("inr")+3,str1.indexOf(" ",4));
			        attributeValue = str2.replaceAll("[^\\d.]", "");
				}
			    	else if (smstxt.contains("rs ")){
			    		str1 = smstxt.substring(smstxt.indexOf("rs "), smstxt.indexOf("rs ") + 20);
				
			    		str2= str1.substring(str1.indexOf("rs ")+3,str1.indexOf(" ",4));
			    		 attributeValue = str2.replaceAll("[^\\d.]", "");
			    	}
				else if (smstxt.contains("rs.")){
			       	 str1 = smstxt.substring(smstxt.indexOf("rs."), smstxt.indexOf("rs.") + 20);
			       	str2= str1.substring(str1.indexOf("rs.")+3,str1.indexOf(" ",5));
			        attributeValue = str2.replaceAll("[^\\d.]", "");
				}
			    } 
		    try {
		    	
		     if (attributeValue.length() > 0) {
		      context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + t + "\t" + attributeName + "\t" + attributeValue +"\t"+circle+ "\t" + operator  +"\t" +fromNumber + "\t"+ Double.toString(priority) +"\t"+usercategory));
		         }
		    } catch (InterruptedException e) {
		     e.printStackTrace();
		    }
			return  String.valueOf(attributeValue);
		   }
	   public String credited(String smstxt, String smsId, String phoneNo,String userid, String time, Context context,String operator,String circle,String fromNumber, String usercategory) throws IOException {
		    attributeName = "credited";
		     attributeValue ="";
		     priority = 1.01;
		    String str1="",str2="";
		    try {
		     if (smstxt.contains("credited") ){
		     
		     if (smstxt.contains(" rs.")){

		    	    str1 = smstxt.substring(smstxt.indexOf("rs."), smstxt.indexOf("rs.") + 20);
		    	   str2= str1.substring(str1.indexOf("rs.")+3,str1.indexOf(" ",4));
		    	   attributeValue = str2.replaceAll("[^\\d.]", "");
		    	}
		    	else if (smstxt.contains("inr ")){
			       	 str1 = smstxt.substring(smstxt.indexOf("inr "), smstxt.indexOf("inr ") + 20);
			       	   str2= str1.substring(str1.indexOf("inr ")+3,str1.indexOf(" ",4));
			       	 attributeValue = str2.replaceAll("[^\\d.]", "");
			       	}
				else if (smstxt.contains("inr")){
			       	 str1 = smstxt.substring(smstxt.indexOf("inr"), smstxt.indexOf("inr") + 20);
			       	   str2= str1.substring(str1.indexOf("inr")+3,str1.indexOf(" ",4));
			       	 attributeValue = str2.replaceAll("[^\\d.]", "");
			       	}
		    	else if (smstxt.contains("rs.")){
			       	 str1 = smstxt.substring(smstxt.indexOf("rs."), smstxt.indexOf("rs.") + 20)	;
			       	   str2= str1.substring(str1.indexOf("rs.")+3,str1.indexOf(" ",5));
			       	 attributeValue = str2.replaceAll("[^\\d.]", "");
			       	}
		    	else if (smstxt.contains("rs ")){
		       	 str1 = smstxt.substring(smstxt.indexOf("rs "), smstxt.indexOf("rs ") + 20);
		       	   str2= str1.substring(str1.indexOf("rs ")+3,str1.indexOf(" ",4));
		       	 attributeValue = str2.replaceAll("[^\\d.]", "");
		       	}
		    } 
		     
		        if (str2.length() > 0){
		         context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue+"\t" +circle+ "\t" + operator   + "\t" +fromNumber + "\t"+ Double.toString(priority) + "\t" +usercategory ));
		       } 
		    }catch (InterruptedException e) {
		        e.printStackTrace();
		       }
			return str2;
		      }
	  
	   public String gender(String smstxt, String smsId, String phoneNo,String userid, String time, Context context,String fromNumber,String operator,String circle,String usercategory) throws IOException {
		   
		   attributeName = "gender";
		    attributeValue = "";
		    try {
		    if (smstxt.contains("hi mr.") ||smstxt.contains("hello mr.") ||smstxt.contains("hey mr.") ||smstxt.contains("dear mr.") || smstxt.contains("heyy mr.") || smstxt.contains("hii mr.") ||
		    		 smstxt.contains("dear sir") ||smstxt.contains("hii sir")||smstxt.contains("hello sir") 
		    		 || smstxt.contains("dear salesperson")||smstxt.contains(" tractor ")  || smstxt.contains("hi sir") || smstxt.contains("hi uncle") || smstxt.contains("hi bhaiya") || smstxt.contains("hi bro") || smstxt.contains("hi brother") || smstxt.contains("hi father") || smstxt.contains("hi papa") || smstxt.contains("hi dad") || smstxt.contains("hi pa") || smstxt.contains("hi dady") || smstxt.contains("hi jiju") || smstxt.contains("hi mama") || smstxt.contains("hi chacha") || smstxt.contains("hi tau") || smstxt.contains("hi bhai") || smstxt.contains("hi ladke") || smstxt.contains("hi mr.") || smstxt.contains("hey sir") || smstxt.contains("hey uncle") || smstxt.contains("hey bhaiya") || smstxt.contains("hey bro") || smstxt.contains("hey didi") || smstxt.contains("hey brother") || smstxt.contains("hey father") || smstxt.contains("hey papa") || smstxt.contains("hey dad") || smstxt.contains("hey pa") || smstxt.contains("hi dady") || smstxt.contains("hey jiju") || smstxt.contains("hey mama") || smstxt.contains("hey chacha") || smstxt.contains("hey tau") || smstxt.contains("hey bhai") || smstxt.contains("hey ladke") || smstxt.contains("hello sir") || smstxt.contains("hello uncle") || smstxt.contains("hello bhaiya") || smstxt.contains("hello bro") || smstxt.contains("hello brother") || smstxt.contains("hello father") || smstxt.contains("hello papa") || smstxt.contains("hello dad") || smstxt.contains("hello pa ") || smstxt.contains("hello dady") || smstxt.contains("hello jiju") || smstxt.contains("hello mama") || smstxt.contains("hello chacha") || smstxt.contains("hello tau") || smstxt.contains("hello bhai") || smstxt.contains("hello ladke") || smstxt.contains("aur mote") || smstxt.contains("aur londe") || (smstxt.contains("kumar") && !smstxt.contains("kumari")) || smstxt.contains(" mohd") || smstxt.contains("mohammad")|| smstxt.contains("mohamad") || smstxt.contains("muhammad") || smstxt.contains(" mohd.")) // gender attributeName
		    {
		     priority = 1.01;
		     attributeValue = "male";
		    }
		    else if (smstxt.startsWith("mr.")) // gender attributeName
		    {
		     priority = 1.01;
		     attributeValue = "male";
		    }
		    
		    else if (smstxt.startsWith("mrs.")) // gender attributeName
		    {
		     priority = 1.01;
		     attributeValue = "female";
		    }
		    
		    else if (smstxt.contains("hi mumma") || smstxt.contains("hi mummy") || smstxt.contains("hi mom") || smstxt.contains("hi di") || smstxt.contains("hi sis") || smstxt.contains("hi didi") || smstxt.contains("hi bua") || smstxt.contains("hi bhabhi") || smstxt.contains("hi behen ") || smstxt.contains("hi behna") || smstxt.contains("hi behenji") ||
		    		smstxt.contains("hi mam")||smstxt.contains("hello mam") ||smstxt.contains("hey mam") || smstxt.contains("heyy mam")||
		    		smstxt.contains("dear madam")||smstxt.contains("hello madam") ||smstxt.contains("hey madam") || smstxt.contains("heyy madam")||
		    		
		    		
		    		smstxt.contains("hi aunty") || smstxt.contains("hi chachi") || smstxt.contains("hi ladki") || smstxt.contains("hi mam") || smstxt.contains("hi madam") || smstxt.contains("hey mumma") || smstxt.contains("hey mummy") || smstxt.contains("hey mom") || smstxt.contains("hey di") || smstxt.contains("hey sis") || smstxt.contains("hey didi") || smstxt.contains("hey bua") || smstxt.contains("hey bhabhi") || smstxt.contains("hey behen ") || smstxt.contains("hey behna") || smstxt.contains("hey behenji") || smstxt.contains("hey aunty") || smstxt.contains("hey chachi") || smstxt.contains("hey ladki") || smstxt.contains("hey mam") || smstxt.contains("hey madam") || smstxt.contains("hey madam") || smstxt.contains("dear mrs.") || smstxt.contains("dear mam") || smstxt.contains("dear madam") || smstxt.contains("dear ms.") || smstxt.contains("aur moti") ) {
		     priority = 1.01;
		     attributeValue = "female";
		     
		    } 

		    else if ((smstxt.contains("dear") || smstxt.contains(" hi") || smstxt.contains(" hey") || smstxt.contains(" hello")) && (smstxt.contains("mr.") || smstxt.contains(" sir") || smstxt.contains( "salesperson ") || smstxt.contains( " uncle ") || smstxt.contains( " bro ") || smstxt.contains( "brother ") || smstxt.contains( "father ") || smstxt.contains( "papa ") || smstxt.contains( "dad ") || smstxt.contains( "pa ") || smstxt.contains( "daddy ") || smstxt.contains( "dad ") || smstxt.contains( " jiju ") || smstxt.contains( "mama ") || smstxt.contains( "chacha ") || smstxt.contains( "tau ") || smstxt.contains( "ladke ")))
			{
				priority = 1.01;
				attributeValue =  "male";
			}

		    else if (( smstxt.contains( " dear") || smstxt.contains( " hi") ||  smstxt.contains( " hello") ||  smstxt.contains( " hey")) && ( smstxt.contains(  "madam ") ||  smstxt.contains( "mam ") ||   smstxt.contains( "sis ") ||  smstxt.contains( "didi ") ||   smstxt.contains( "mumma ") ||   smstxt.contains( "bua ")    ||   smstxt.contains( "bhabhi ")    ||    smstxt.contains( "behen ")    ||    smstxt.contains( "behna ")    ||    smstxt.contains( "behenji ")    ||    smstxt.contains( "aunty ")    ||    smstxt.contains( "chachi ")    ||    smstxt.contains( "ladki ")    ||     smstxt.contains( "mummy ")    ||    smstxt.contains( "mom ")    ||    smstxt.contains( "di ")    ||   smstxt.contains( "aunty ")    ||    smstxt.contains( " mrs.")   ||  smstxt.contains( " ms.")))
		    {
		    	priority = 1.01;
				attributeValue = "female";
		    }

		    else if ((smstxt.startsWith("hello")||smstxt.startsWith("hi")||smstxt.startsWith("dear")||smstxt.startsWith("hey") ||smstxt.startsWith("dr. "))    && smstxt.contains(" kumar "))
		    {
		    	priority = 1.04;
		    	attributeValue = "male";
		    }
		    else if ((smstxt.startsWith("hello")||smstxt.startsWith("hi")||smstxt.startsWith("dear")||smstxt.startsWith("hey") ||smstxt.startsWith("dr. "))    && smstxt.contains(" kumari "))
		    {
		    	priority = 1.04;
		    	attributeValue = "female";
		    }
		    
		    
		    
		    
		    else if (( smstxt.contains(" dear") || smstxt.contains(" hi") ||  smstxt.contains( " hello")) &&  !(smstxt.contains( "customer") || smstxt.contains("investor ") ||  smstxt.contains( "counseling ") ||   smstxt.contains( "hello we") ||  smstxt.contains( "hello user") ||   smstxt.contains( "hello xyz") ))
			{
		    	priority = 2.01;
				int len, k, r;
			    
			     r = smstxt.indexOf("dear ") + 5;
			     r = smstxt.indexOf("hi ") + 3;
			     r = smstxt.indexOf("hello ") + 6;
			     len = smstxt.length();
			     for (k = r; k < len; k++) {
			      if (smstxt.charAt(k) == ' ' || smstxt.charAt(k) == ',') {
			       char c = smstxt.charAt(k - 1);
			       String s = String.valueOf(c);
			       
			       if ((c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u') && Pattern.matches("[a-zA-Z]+", s)) {
			        attributeValue = "female";	        
			       } 
			       else {
			        attributeValue = "male";	        
			       }
			       break;
			      }
			     }
			}

		else if ((smstxt.startsWith("hi ")) && (!smstxt.contains("hi loan") && !smstxt.contains("hi, wiproite") && !smstxt.contains("hi .y") && !smstxt.contains("hi thanks") && !smstxt.contains("hi ! y") && !smstxt.contains("hi hfrp") && !smstxt.contains("hi tssss") && !smstxt.contains("hi this") && !smstxt.contains("hi there") && !smstxt.contains("hi test") && !smstxt.contains("hi the") && !smstxt.contains("hi (name)") && !smstxt.contains("hi -wel") && !smstxt.contains("hi - wel") && !smstxt.contains("hi - your") && !smstxt.contains("hi . Your"))) {
		        priority = 2.01;
		        int len, k, r;
		     r = smstxt.indexOf(" hi ") + 3;

		     len = smstxt.length();
		     for (k = r; k < len; k++) {
		      if (smstxt.charAt(k) == ' ' || smstxt.charAt(k) == ',' || smstxt.charAt(k) == '!' ) {
		       char c = smstxt.charAt(k - 1);
		       String s = String.valueOf(c);
		       if ((c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u') && Pattern.matches("[a-zA-Z]+", s)) {
		        attributeValue = "female";
		       } else {
		        attributeValue = "male";
		       }
		       break;

		      }
		     }
		    } 
		    else if ((smstxt.startsWith("hello ")) && (!smstxt.contains("hello, the") && !smstxt.contains("hello, wiproite") && !smstxt.contains("hello!we") && !smstxt.contains("hello!you") && !smstxt.contains("hello abcd") && !smstxt.contains("hello!subsribe") && !smstxt.contains("hello,we") && !smstxt.contains("helloyour") && !smstxt.contains("hello! t") && !smstxt.contains("hello t ") && !smstxt.contains("hello member") && !smstxt.contains("hello allotment") && !smstxt.contains("hello atm") && !smstxt.contains("hello, you") && !smstxt.contains("hello magzine") && !smstxt.contains("hello,your") && !smstxt.contains("hello, this") && !smstxt.contains("hello xyz") && !smstxt.contains("hello,a travel") && !smstxt.contains("hello, one") && !smstxt.contains("hello, kindly") && !smstxt.contains("hello. Please") && !smstxt.contains("hello, credit") && !smstxt.contains("hello@") && !smstxt.contains("hello, please") && !smstxt.contains("hello, item") && !smstxt.contains("hello from") && !smstxt.contains("hello, i") && !smstxt.contains("hello all") && !smstxt.contains("hello! Your") && !smstxt.contains("hello!we") && !smstxt.contains("hello all") && !smstxt.contains("hello,as") && !smstxt.contains("hello. kindly"))) {
		        priority = 2.01;
		     int len, k, r;
		     r = smstxt.indexOf("hello ") + 6;

		     len = smstxt.length();
		     for (k = r; k < len; k++) {
		      if (smstxt.charAt(k) == ' ' || smstxt.charAt(k) == ',') {
		       char c = smstxt.charAt(k - 1);
		       String s = String.valueOf(c);
		       if ( (c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u')&& Pattern.matches("[a-zA-Z]+", s)) {
		        attributeValue = "female";
		       } else {
		        attributeValue = "male";
		       }
		       

		      } 
		     }
		    }
		   
		    else if ((smstxt.startsWith("dear ")) && (!smstxt.contains("dear customer") && !smstxt.contains("dear investor") && !smstxt.contains("dear counseling") && !smstxt.contains("dear valued") && !smstxt.contains("dear club") && !smstxt.contains("dear guest") && !smstxt.contains("dear incumbent") && !smstxt.contains("dear aviva") && !smstxt.contains("dear sir/mam") && !smstxt.contains("dear axis") && !smstxt.contains("dear partner") && !smstxt.contains("dear iterm") && !smstxt.contains("dear cbm/bm") && !smstxt.contains("dear msdian") && !smstxt.contains("dear csp") && !smstxt.contains("dear policyholder") && !smstxt.contains("dear channel") && !smstxt.contains("dear d2h") && !smstxt.contains("dear lvb") && !smstxt.contains("dear administrator") && !smstxt.contains("dear associate") && !smstxt.contains("dear parent") && !smstxt.contains("dear sir/madam") && !smstxt.contains("dear student") && !smstxt.contains("dear met") && !smstxt.contains("dear tmf") && !smstxt.contains("dear your") && !smstxt.contains("dear abcd") && !smstxt.contains("dear aegon") && !smstxt.contains("dear aspirant") && !smstxt.contains("dear asm") && !smstxt.contains("dear auditor") && !smstxt.contains("dear auro") && !smstxt.contains("dear bfl") && !smstxt.contains("dear bh") && !smstxt.contains("dear bussiness") && !smstxt.contains("dear candidates") && !smstxt.contains("dear cl84") && !smstxt.contains("dear crew") && !smstxt.contains("dear cust") && !smstxt.contains("dear dealor") && !smstxt.contains("dear deposit") && !smstxt.contains("dear dolphin") && !smstxt.contains("dear donor") && !smstxt.contains("dear dp") && !smstxt.contains("dear dotcabs") && !smstxt.contains("dear driver") && !smstxt.contains("dear donor") 
		            &&!smstxt.contains("dear emp") && !smstxt.contains("dear fac") && !smstxt.contains("dear fino") && !smstxt.contains("dear forum") && !smstxt.contains("dear friend") && !smstxt.contains("dear gamebuddy") && !smstxt.contains("dear gas") && !smstxt.contains("dear gsc") && !smstxt.contains("dear gold") && !smstxt.contains("dear guardian") && !smstxt.contains("dear health") && !smstxt.contains("dear host") && !smstxt.contains("dear indigo") && !smstxt.contains("dear instructor") && !smstxt.contains("dear jalan") && !smstxt.contains("dear key") && !smstxt.contains("dear kids") && !smstxt.contains("dear la") && !smstxt.contains("dear learner") && !smstxt.contains("dear fac") && !smstxt.contains("dear maben") && !smstxt.contains("dear member") && !smstxt.contains("dear passenger") && !smstxt.contains("dear pensioner") && !smstxt.contains("dear phama") && !smstxt.contains("dear prerana") && !smstxt.contains("dear principal") && !smstxt.contains("dear r.m") && !smstxt.contains("dear relationship") && !smstxt.contains("dear resident") && !smstxt.contains("dear retailer") && !smstxt.contains("dear rhs") && !smstxt.contains("dear rh") && !smstxt.contains("dear rm") && !smstxt.contains("dear rs name") && !smstxt.contains("dear rupantaran") && !smstxt.contains("dear seller") && !smstxt.contains("dear sir / madam") && !smstxt.contains("dear sm/oh") && !smstxt.contains("dear spice") && !smstxt.contains("dear so,") && !smstxt.contains("dear sp,") && !smstxt.contains("dear sri sai") && !smstxt.contains("dear staff") && !smstxt.contains("dear stockholding") && !smstxt.contains("dear subscriber") && !smstxt.contains("dear surveyor") && !smstxt.contains("dear teacher") && !smstxt.contains("dear team") && !smstxt.contains("dear tech") && !smstxt.contains("dear tmf") && !smstxt.contains("dear test") && !smstxt.contains("dear tpddl") && !smstxt.contains("dear tractor") && !smstxt.contains("dear trainer") && !smstxt.contains("dear trustee") && !smstxt.contains("dear ubiuser") 
		            && !smstxt.contains("dear ucoites") && !smstxt.contains("dear user") && !smstxt.contains("dear vaish") 
		            && !smstxt.contains("dear valuefirst") && !smstxt.contains("dear vb") && !smstxt.contains("dear vendor") 
		            && !smstxt.contains("dear visitor")  && !smstxt.contains("dear viproite") 
		            && !smstxt.contains("dear xxx") && !smstxt.contains("dear xyz") && !smstxt.contains("dear zh") 
		            && !smstxt.contains("dear member") && !smstxt.contains("dear privilege") && !smstxt.contains("dear patient") && !smstxt.contains("dear prof") && !smstxt.contains("dear rbm") 
		            && !smstxt.contains("dear reader") && !smstxt.contains("dear saving") && !smstxt.contains("dear travel") && !smstxt.contains("dear trustline") && !smstxt.contains("dear vp") && !smstxt.contains("dear consumer") && !smstxt.contains("dear pnm") && !smstxt.contains("dear distributor") && !smstxt.contains("dear colleague") && !smstxt.contains("dear <<xyz>>")  && !smstxt.contains("dear <>") 
		            && !smstxt.contains("dear __") && !smstxt.contains("dear abc") && !smstxt.contains("dear admin") && !smstxt.contains("dear advisor") && !smstxt.contains("dear adsf") && !smstxt.contains("dear agent") && !smstxt.contains("dear agr74") && !smstxt.contains("dear and7") && !smstxt.contains("dear and8") && !smstxt.contains("dear applicant") && !smstxt.contains("dear arli") && !smstxt.contains("dear bsc") && !smstxt.contains("dear bdm") 
		            && !smstxt.contains("dear beneficiary") && !smstxt.contains("dear bm") 
		            && !smstxt.contains("dear branch") && !smstxt.contains("dear broker") && !smstxt.contains("dear bsm")
		            && !smstxt.contains("dear buisness") && !smstxt.contains("dear cabin")
		            && !smstxt.contains("dear call") && !smstxt.contains("dear sir/mam")
		            && !smstxt.contains("dear customer,")&& !smstxt.contains("dear card") &&  !smstxt.contains("dear, wiproite") 
		            && !smstxt.contains("dear cavins") && !smstxt.contains("dear manager"))) {
		        priority = 2.01;
		     int len, k, r;
		    
		     r = smstxt.indexOf("dear ") + 5;

		     len = smstxt.length();
		     for (k = r; k < len; k++) {
		      if (smstxt.charAt(k) == ' ' || smstxt.charAt(k) == ',') {
		       char c = smstxt.charAt(k - 1);
		       String s = String.valueOf(c);
		       
		       if ((c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u') && Pattern.matches("[a-zA-Z]+", s)) {
		        attributeValue = "female";
		       } else {
		        attributeValue = "male";
		       }
		       break;
		      }
		     }
		    } 

		    else if ((smstxt.contains(" dr") || smstxt.contains(" dr."))  &&  (smstxt.contains("appointment")) && (fromNumber.equals("practo"))  && (smstxt.contains("with patient"))){
		    	priority=2.03;

		    	     int len, k, r;
		    	    
		    	     r = smstxt.indexOf("dr.") + 4;
		    	     r = smstxt.indexOf("dr") + 3;

		    	     len = smstxt.length();
		    	     for (k = r; k < len; k++) {
		    	      if (smstxt.charAt(k) == ' ' ) {
		    	       char c = smstxt.charAt(k - 1);
		    	       String s = String.valueOf(c);
		    	       
		    	       if ((c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u') && Pattern.matches("[a-zA-Z]+", s)) {
		    	        attributeValue = "female";
		    	       } else {
		    	        attributeValue = "male";
		    	       }
		    	       break;
		    	      }
		    	     }
		    	    	}
		else if ((smstxt.contains("congratulations")  && smstxt.contains("matrimony")) && !(smstxt.contains("congratulations user") || smstxt.contains("congratulations xyz") || smstxt.contains("congratulations!"))){
		priority=2.04;

		     int len, k, r;
		    
		     r = smstxt.indexOf("congratulations ") + 15;

		     len = smstxt.length();
		     for (k = r; k < len; k++) {
		      if (smstxt.charAt(k) == ' ' ) {
		       char c = smstxt.charAt(k - 1);
		       String s = String.valueOf(c);
		       
		       if ((c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u') && Pattern.matches("[a-zA-Z]+", s)) {
		        attributeValue = "male";
		       } else {
		        attributeValue = "female";
		       }
		       break;
		      }
		     }
		    	}

		else if(smstxt.contains("i'm") && smstxt.contains("matrimony"))
		{
			priority = 2.05;
			int len, k, r;

			r = smstxt.indexOf("i'm") + 4;

			len = smstxt.length();
			for (k = r; k < len; k++) {
			 
			if (smstxt.charAt(k) == ' ' || smstxt.charAt(k) == ',') {
			char c = smstxt.charAt(k - 1);
			String s = String.valueOf(c);
			

			if ((c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u') && Pattern.matches("[a-zA-Z]+", s)) {
			attributeValue = "female";
			} else {
			attributeValue = "male";
			}
			break;
			}
			}
		}
		else if (smstxt.contains("msg from") && smstxt.contains("interest"))
		{
			priority = 2.06;
			int len, k, r;

			r = smstxt.indexOf("msg from") + 9;

			len = smstxt.length();
			for (k = r; k < len; k++) {
			 
			if (smstxt.charAt(k) == ' ' || smstxt.charAt(k) == ',') {
			char c = smstxt.charAt(k - 1);
			String s = String.valueOf(c);
			

			if ((c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u') && Pattern.matches("[a-zA-Z]+", s)) {
			attributeValue = "female";
			} else {
			attributeValue = "male";
			}
			break;
			}
			}
		}
		else if (
				  ( smstxt.contains("pickup") || smstxt.contains("pick-up")) 
				  && smstxt.contains("drop ") && smstxt.contains("supervisor ")  || ( smstxt.contains("customer") && (smstxt.contains("pick-up") || smstxt.contains("pickup"))
						  && smstxt.contains("drop ") || smstxt.contains("dear driver")  ) 
				){
		     priority = 2.07;
			      
		   attributeValue="male";
			}	
		else  if (smstxt.contains("dear driver") ) {

			priority = 2.08;
			attributeValue="male";
		}
		else if (smstxt.contains("interest") && smstxt.contains(" her ")  ){
			     priority = 2.09;
			      
			   attributeValue="male";
			}
		else if (smstxt.contains("interest") && smstxt.contains(" him ")  ){
		     priority = 2.11;
			      
		   attributeValue="female";
			}


		else if(smstxt.contains("dear partner") && userid.equals("bajajauto") ){
			attributeValue = "male";
			priority = 2.12;
		}  

		else if ((smstxt.contains("for men ") || smstxt.contains("polos ") || smstxt.contains("sherwani ") || smstxt.contains("kurtas ") || smstxt.contains("condoms ") || smstxt.contains("condom ")) && (smstxt.contains("ordered ") || smstxt.contains("dispatched ") || smstxt.contains("delievered") || smstxt.contains("order no")))
		{
			priority = 3.01;
			attributeValue = "male";
		}
		else if ((smstxt.contains("for women") || smstxt.contains("skirt ") || smstxt.contains("saree ") || smstxt.contains("leggings ") || smstxt.contains("liengries ")) && (smstxt.contains("ordered ") || smstxt.contains("dispatched ") || smstxt.contains("delievered ") || smstxt.contains("order no")))
		{
			priority = 3.02;
			attributeValue = "female";
		}

		else if (smstxt.contains("your password") || smstxt.contains("registered") || smstxt.contains("registering") && fromNumber.equals("WPLUPP") ){
		  	     priority = 3.03;
				      
		  	   attributeValue="female";
		    	}
		else if (smstxt.contains("complaint ") && smstxt.contains("against you") && fromNumber.equals("WPLUPP") ){
		  	     priority = 3.04;
				      
		  	   attributeValue="male";
		    	}
		    
		    if (attributeValue.length() > 0) {
		    	
		        context.write(new Text(smsId+"-"+attributeName+"-"+attributeValue+"-"+Double.toString(priority)), new Text(smsId + "\t" + phoneNo +"\t"+ userid+ "\t" + time + "\t" + attributeName + "\t" + attributeValue +"\t"+circle+ "\t" + operator +"\t"  + fromNumber + "\t"+ Double.toString(priority)+ "\t" +usercategory));
		       }
		  
		    } catch (InterruptedException e) {
		     // TODO Auto-generated catch block
		     e.printStackTrace();
		    }return  String.valueOf(attributeValue) ;  
		   }
		  }