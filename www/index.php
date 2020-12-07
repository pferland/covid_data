<?php

$dir    = '/var/www/covid/charts';
$files1 = scandir($dir);

$i = 0;
foreach($files1 as $file)
{
	if($file == "." or $file == "..")
	{
		continue;
	}
	$category = explode(".", $file)[0];
	echo "<a href='charts/".$category."/'>".$category."</a></br>";
}


$mtime = filemtime($dir."/Primary_City-Town_Residence/Boston.png");
$timezone = new DateTimeZone('US/Eastern');
$dt = new DateTime("@$mtime");
$dt->setTimezone($timezone);
echo "<p> Last Update: ".$dt->format('r')."</p>";
