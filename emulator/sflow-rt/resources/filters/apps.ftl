<!DOCTYPE html>
<html lang="en">
<head>
<title>sFlow-RT Applications</title>
<meta charset="utf-8">
<meta name="google" content="notranslate">
<link rel="stylesheet" href="${root}inc/inmsf/main.css" type="text/css" media="all">
<link rel="icon" type="image/png" href="${root}inc/img/favicon.png">
</head>
<body>
<div id="titleBar"><div id="product"><span id="logo"></span>sFlow-RT</div></div>
<div id="content">
<h1>Installed Applications</h1>
<#if apps?keys[0]??>
<p>Click on an application in the list to access the application's home page:</p>
<#assign names = apps?keys?sort>
<ul>
<#list names as name>
<li>
<#if apps[name].entry??>
<a href="${name}${apps[name].entry}">${name}</a>
<#else>
${name}
</#if>
</li>
</#list>
</ul>
<#else>
<p><i>No applications installed</i></p>
<p>Visit <a href="https://sflow-rt.com/">sFlow-RT.com</a> to find applications,  learn how to author applications, and connect with the sFlow-RT developer and user community.</p>
</#if>
</div>
<#include "resources/filters/footer.ftl">
