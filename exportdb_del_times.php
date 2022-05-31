<?php
// ini_set("memory_limit","-1");
$sid = $_REQUEST['sid'];
$filename = $_REQUEST['filelink'];
$ydate = $_REQUEST['ydate'];
//$filename = $sid.'_'.$todate.'.csv';
/*header("Content-type: text/csv");
header("Content-Disposition: attachment; filename=$filename");
header("Pragma: no-cache");
header("Expires: 0");*/

 $dbhost="DB_HOST";
 $dbuser="DB_USER";
 $dbpass="DB_PASSWORD";
 $database="DB_NAME";

 $conn = mysqli_connect($dbhost,$dbuser,$dbpass,$database);
   $query = "SELECT * FROM TABLE_NAME where senderid='".$sid."' and entrytime like '".$ydate."%' order by id asc";  
if (!$result = mysqli_query($conn, $query)) {
    exit(mysqli_error($conn));
}

 $rowcount=mysqli_num_rows($result);
 //$sql = "update senderids set count='".$rowcount."' where senderid='".$sid."' and campaigndate like '".$ydate."%'";
 //mysqli_query($conn,$sql);


$title = array("senderid", "entrytime", "receiver", "content", "status");
$i = 0;
$p = 0;
while($rs = mysqli_fetch_array($result, MYSQLI_ASSOC)){
	$content = array();
	
	if($i == 0){
		echo "$i file createing";
		$fp = fopen('exports/'.$sid."_times_".$ydate."_".$p.".csv", 'w');		
		fputcsv($fp, $title);

		////
		$fp1 = fopen('exports/'.$sid."_times_m_".$ydate."_".$p.".csv", 'w');
		fputcsv($fp1, $title);
		$p++;
	}

	//foreach ($results as $rs) {
		$row = array();
		$row1 = array();
	 
		$row[] = stripslashes($rs["senderid"]);
		$row[] = stripslashes($rs["entrytime"]);
		//$row[] = stripslashes($rs["receiver"]);
		$row[] = stripslashes(substr($rs["receiver"], -10));
		$row[] = stripslashes($rs["content"]);
		$row[] = stripslashes($rs["status"]);
		
		$row1[] = stripslashes($rs["senderid"]);
		$row1[] = stripslashes($rs["entrytime"]);
		$row1[] = 'XXXXX'.substr(stripslashes(substr($rs["receiver"], -10)), -5);
		$row1[] = stripslashes($rs["content"]);
		$row1[] = stripslashes($rs["status"]);
		
		//$content[] = $row;
		fputcsv($fp, $row);
		fputcsv($fp1, $row1);
	//}
	
	if(($i !=0) && ($i%900000 == 0)) {
		fclose($fp);
		fclose($fp1);
		echo "$i file createing";
		$fp = fopen('exports/'.$sid."_times_".$ydate."_".$p.".csv", 'w');		
		fputcsv($fp, $title);

		////
		$fp1 = fopen('exports/'.$sid."_times_m_".$ydate."_".$p.".csv", 'w');
		fputcsv($fp1, $title);
		$p++;
	}
	$i++;

}
fclose($fp);
fclose($fp1);
echo "export to csv line executed.";

