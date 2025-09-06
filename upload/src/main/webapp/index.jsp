<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>文件上传测试</title>
</head>
<body>
	<form action="/upload/UploadifyAvatarServlet" enctype="multipart/form-data" method="post">
		<input type="hidden" name="userId" value="10047" />
		<input type="hidden" name="version" value="1" />
		<table>
			<tr><td>头像：<input type="file" name="image"></td></tr>
			<tr><td><input type="submit" value="头像上传" /></td></tr>
		</table>
	</form>
	<form action="/upload/UploadServlet" enctype="multipart/form-data"
		method="post">
		<input type="hidden" name="uploadFlag" value="1" /> 
		<input type="hidden" name="userId" value="100123" />
		<input type="hidden" name="validTime" value="10" />
		<table>
			<tr><td>文件1：<input type="file" name="files"></td></tr>
			<tr><td>文件2： <input type="file" name="files"></td></tr>
			<tr><td>文件3：<input type="file" name="files"></td></tr>
			<tr><td>文件4： <input type="file" name="files"></td></tr>
			<tr><td><input type="submit" value="照片上传" /></td></tr>
		</table>
	</form>

	<form action="/upload/UploadServlet" enctype="multipart/form-data"
		method="post">
		<input type="hidden" name="uploadFlag" value="2" />
		<input type="hidden" name="userId" value="100123" />
		<input type="hidden" name="validTime" value="10" />
		<table>
			<tr><td>文件1：<input type="file" name="file1"></td></tr>
			<tr><td>文件2： <input type="file" name="file2"></td></tr>
			<tr><td><input type="submit" value="商务圈图片上传" /></td></tr>
		</table>
	</form>

	<form action="/upload/UploadServlet" enctype="multipart/form-data"
		method="post">
		<input type="hidden" name="uploadFlag" value="3" />
		<input type="hidden" name="userId" value="100123" />
		<input type="hidden" name="validTime" value="10" />
		<table>
			<tr><td>文件1：<input type="file" name="file1"></td></tr>
			<tr><td>文件2： <input type="file" name="file2"></td></tr>
			<tr><td><input type="submit" value="图片上传" /></td></tr>
		</table>
	</form>

	<form action="/upload/UploadServlet" enctype="multipart/form-data"
		method="post">
		<input type="hidden" name="uploadFlag" value="4" />
		<input type="hidden" name="userId" value="100123" />
		<input type="hidden" name="validTime" value="10" />
		<table>
			<tr><td>文件1：<input type="file" name="file1"></td></tr>
			<tr><td>文件2： <input type="file" name="file2"></td></tr>
			<tr><td><input type="submit" value="视频上传" /></td></tr>
		</table>
	</form>

	<form action="/upload/UploadServlet" enctype="multipart/form-data"
		method="post">
		<input type="hidden" name="uploadFlag" value="4" />
		<input type="hidden" name="userId" value="100123" />
		<input type="hidden" name="validTime" value="10" />
		<table>
			<tr><td>文件1：<input type="file" name="file1"></td></tr>
			<tr><td>文件2： <input type="file" name="file2"></td></tr>
			<tr><td><input type="submit" value="其它上传" /></td></tr>
		</table>
	</form>
	
	<form action="/upload/UploadMusicServlet" enctype="multipart/form-data" method="post">
		<input type="hidden" name="uploadFlag" value="4" />
		<input type="hidden" name="userId" value="100123" />
		<input type="hidden" name="validTime" value="10" />
		<table>
			<tr><td>文件1：<input type="file" name="file1"></td></tr>
			<tr><td>文件2： <input type="file" name="file2"></td></tr>
			<tr><td><input type="submit" value="短视频音乐上传" /></td></tr>
		</table>
	</form>

	<form action="/upload/categoryServlet" enctype="multipart/form-data"
		  method="post">
		<input type="hidden" name="uploadFlag" value="4" />
		<input type="hidden" name="categoryId" value="100123" />
		<table>
			<tr><td>文件1：<input type="file" name="file1"></td></tr>
			<tr><td><input type="submit" value="商品分类上传" /></td></tr>
		</table>
	</form>

	<form action="/upload/GroupAvatarServlet" enctype="multipart/form-data" method="post">
		<input type="hidden" name="jid" value="10012311112545" />
		<table>
			<tr><td>文件1：<input type="file" name="file1"></td></tr>
			<tr><td><input type="submit" value="群组自定义头像" /></td></tr>
		</table>
	</form>
	<form action="/upload/UploadAvatarServlet" enctype="multipart/form-data"
		  method="post">
		<input type="hidden" name="userId" value="23243242" />
		<table>
			<tr><td>文件1：<input type="file" name="file1"></td></tr>
			<tr><td><input type="submit" value="自定义头像" /></td></tr>
		</table>
	</form>

	<form action="/upload/UploadAvatarServletByIds" enctype="multipart/form-data" method="post">
		<input type="hidden" name="userId" value="23243242" />
		<input type="hidden" name="version" value="1" />
		<table>
			<tr><td>文件1：<input type="file" name="image"></td></tr>
			<tr><td><input type="submit" value="自定义头像ByIds" /></td></tr>
		</table>
	</form>

	<form action="/upload/UploadVoiceServlet" enctype="multipart/form-data" method="post">
		<input type="hidden" name="userId" value="23243242" />
		<input type="hidden" name="validTime" value="2433" />
		<table>
			<tr><td>文件1：<input type="file" name="files"></td></tr>
			<tr><td><input type="submit" value="视频上传" /></td></tr>
		</table>
	</form>


<h1>MD5测试</h1>
<form action="/file/v1/check/md5" enctype="multipart/form-data" method="post">
	<table>
		<tr><td>文件1：<input type="file" name="files"></td></tr>
		<tr><td>文件1：<input type="file" name="files"></td></tr>
		<tr><td><input type="submit" value="检测" /></td></tr>
	</table>
</form>


<h1>新上传接口测试</h1>
<form action="/upload/uploadServlet" enctype="multipart/form-data" method="post">
	<input type="hidden" name="uploadFlag" value="1" />
	<input type="hidden" name="userId" value="100123" />
	<input type="hidden" name="validTime" value="10" />
	<table>
		<tr><td>文件1：<input type="file" name="files"></td></tr>
		<tr><td>文件2： <input type="file" name="files"></td></tr>
		<tr><td>文件3：<input type="file" name="files"></td></tr>
		<tr><td>文件4： <input type="file" name="files"></td></tr>
		<tr><td><input type="submit" value="照片上传" /></td></tr>
	</table>
</form>

<form action="/upload/uploadServlet" enctype="multipart/form-data"
	  method="post">
	<input type="hidden" name="uploadFlag" value="2" />
	<input type="hidden" name="userId" value="100123" />
	<input type="hidden" name="validTime" value="10" />
	<table>
		<tr><td>文件1：<input type="file" name="files"></td></tr>
		<tr><td>文件2： <input type="file" name="files"></td></tr>
		<tr><td><input type="submit" value="商务圈图片上传" /></td></tr>
	</table>
</form>

<form action="/upload/uploadServlet" enctype="multipart/form-data" method="post">
	<input type="hidden" name="uploadFlag" value="3" />
	<input type="hidden" name="userId" value="100123" />
	<input type="hidden" name="validTime" value="10" />
	<table>
		<tr><td>文件1：<input type="file" name="files"></td></tr>
		<tr><td>文件2： <input type="file" name="files"></td></tr>
		<tr><td><input type="submit" value="图片上传" /></td></tr>
	</table>
</form>
<form action="/upload/uploadAvatarServlet" enctype="multipart/form-data" method="post">
	<input type="hidden" name="userId" value="23243242" />
	<table>
		<tr><td>文件1：<input type="file" name="files"></td></tr>
		<tr><td><input type="submit" value="自定义头像" /></td></tr>
	</table>
</form>
<form action="/upload/groupAvatarServlet" enctype="multipart/form-data" method="post">
	<input type="hidden" name="uploadFlag" value="4" />
	<input type="hidden" name="jid" value="groupjid" />
	<table>
		<tr><td>文件1：<input type="file" name="files"></td></tr>
		<tr><td><input type="submit" value="群组头像上传" /></td></tr>
	</table>
</form>
<form action="/upload/updateGroupAvatar" enctype="multipart/form-data" method="post">
	<input type="hidden" name="jid" value="groupjid" />
	<input type="hidden" name="userIds" value="1004000200,1000022,10000002,10010002,10020002" />
	<table>
		<tr><td>文件1：<input type="file" name="files"></td></tr>
		<tr><td><input type="submit" value="更新群组头像" /></td></tr>
	</table>
</form>

<form action="/upload/uploadServlet" enctype="multipart/form-data" method="post">
	<input type="hidden" name="uploadFlag" value="4" />
	<input type="hidden" name="userId" value="100123" />
	<input type="hidden" name="validTime" value="10" />
	<table>
		<tr><td>文件1：<input type="file" name="files"></td></tr>
		<tr><td>文件2： <input type="file" name="files"></td></tr>
		<tr><td><input type="submit" value="视频上传" /></td></tr>
	</table>
</form>

<form action="/upload/uploadServlet" enctype="multipart/form-data" method="post">
	<input type="hidden" name="uploadFlag" value="4" />
	<input type="hidden" name="userId" value="100123" />
	<input type="hidden" name="validTime" value="10" />
	<table>
		<tr><td>文件1：<input type="file" name="files"></td></tr>
		<tr><td>文件2： <input type="file" name="files"></td></tr>
		<tr><td><input type="submit" value="其它上传" /></td></tr>
	</table>
</form>


<form action="/upload/categoryServlet" enctype="multipart/form-data" method="post">
	<input type="hidden" name="uploadFlag" value="4" />
	<input type="hidden" name="categoryId" value="100123" />
	<table>
		<tr><td>文件1：<input type="file" name="files"></td></tr>
		<tr><td><input type="submit" value="商品分类上传" /></td></tr>
	</table>
</form>





<form action="/upload/uploadAvatarServletByIds" enctype="multipart/form-data" method="post">
	<input type="hidden" name="userId" value="23243242" />
	<input type="hidden" name="version" value="1" />
	<table>
		<tr><td>文件1：<input type="file" name="image"></td></tr>
		<tr><td><input type="submit" value="自定义头像ByIds" /></td></tr>
	</table>
</form>

	<form action="/upload/uploadifyServlet" enctype="multipart/form-data" method="post">
		<input type="hidden" name="userId" value="10047" />
		<input type="hidden" name="version" value="1" />
		<table>
			<tr><td>头像：<input type="file" name="files"></td></tr>
			<tr><td><input type="submit" value="头像上传" /></td></tr>
		</table>
	</form>

	<form action="/upload/uploadMusicServlet" enctype="multipart/form-data" method="post">
		<input type="hidden" name="uploadFlag" value="4" />
		<input type="hidden" name="userId" value="100123" />
		<input type="hidden" name="validTime" value="10" />
		<table>
			<tr><td>文件1：<input type="file" name="files"></td></tr>
			<tr><td>文件2： <input type="file" name="files"></td></tr>
			<tr><td><input type="submit" value="短视频音乐上传" /></td></tr>
		</table>
	</form>

	<form action="/upload/uploadVoiceServlet" enctype="multipart/form-data" method="post">
		<input type="hidden" name="userId" value="23243242" />
		<input type="hidden" name="validTime" value="0" />
		<table>
			<tr><td>文件1：<input type="file" name="files"></td></tr>
			<tr><td><input type="submit" value="音频上传" /></td></tr>
		</table>
	</form>


</body>
</html>