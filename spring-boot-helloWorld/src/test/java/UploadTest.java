import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.ExecutorFactory;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.entity.ContentType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;

/**
 * S3 文件操作
 */
public class UploadTest {

    // region 常量定义

    /**
     * ACCESSKEY
     */
    private static final String ACCESSKEY = "";
    /**
     * SECRETKEY
     */
    private static final String SECRETKEY = "";
    /**
     * 存储桶名称
     */
    private static final String BUCKET_NAME = "";
    /**
     * 创建s3对象
     */
    private static final BasicAWSCredentials awsCreds = new BasicAWSCredentials(ACCESSKEY, SECRETKEY);
    private static final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
            .withRegion(Regions.AP_NORTHEAST_1)
            .build();

    // endregion

    // region 公共方法
    /**
     * 上传文件
     *
     * @param file       文件
     * @param fileName   文件名（如果为null，则使用UUID生成）
     * @param updatePath 上传路径[ eg: xxx/xxx ]
     */
    public static String updateLoadFile(MultipartFile file, String fileName, String updatePath) {

        if (isEmpty(file)) {
            return null;
        }

        String oldFileName = file.getOriginalFilename();
        String eName = oldFileName.substring(oldFileName.lastIndexOf("."));
        String newFileName = (StringUtils.isEmpty(fileName) ? UUID.randomUUID() : fileName) + eName;
        try {
            File localFile = File.createTempFile("temp", null);
            file.transferTo(localFile);

            // 指定要上传到服务器上的路径
            String key = updatePath + newFileName;

            // 设置文件并设置公读
//            ObjectMetadata metadata = new ObjectMetadata();
//            metadata.setContentType("image/png");
//            metadata.addUserMetadata("x-amz-meta-title", "someTitle");
            PutObjectRequest request = new PutObjectRequest(BUCKET_NAME, key, localFile);
//            request.setMetadata(metadata);

            request.withCannedAcl(CannedAccessControlList.PublicRead);
            // 上传文件
            PutObjectResult putObjectResult = s3.putObject(request);
            if (StringUtils.isNotEmpty(putObjectResult.getETag())) {
                return key;
            }
            return null;

        } catch (IOException e) {
        } catch (Exception e) {
        }
        return null;
    }

    /**
     * 删除单个文件
     *
     * @param filePath 文件路径[ eg: /head/xxxx.jpg ]
     * @return
     */
    public static void deleteObject(String filePath) {

        if (filePath.startsWith("/")) {
            filePath = filePath.substring(1);
        }

        try {
            s3.deleteObject(BUCKET_NAME, filePath);
        } catch (Exception e) {
        }
    }

    /**
     * 删除文件夹
     *
     * @param filePath  文件夹地址[ eg:temp/1 或 temp ]
     * @param deleteAll true-递进删除所有文件（包括子文件夹）；false-只删除当前文件夹下的文件，不删除子文件夹内容
     */
    public static void deleteFolder(String filePath, boolean deleteAll) {

        ListObjectsV2Request objectsRequest = new ListObjectsV2Request();
        objectsRequest.setBucketName(BUCKET_NAME);
        objectsRequest.setPrefix(filePath);
        // deliter表示分隔符, 设置为/表示列出当前目录下的object, 设置为空表示列出所有的object
        objectsRequest.setDelimiter(deleteAll ? "" : "/");
        // 设置最大遍历出多少个对象, 一次listobject最大支持1000
        objectsRequest.setMaxKeys(1000);

        ListObjectsV2Result listObjectsRequest = s3.listObjectsV2(objectsRequest);

        List<S3ObjectSummary> objects = listObjectsRequest.getObjectSummaries();
        String[] object_keys = new String[objects.size()];
        for (int i = 0; i < objects.size(); i++) {
            S3ObjectSummary item = objects.get(i);
            object_keys[i] = item.getKey();
        }

        try {
            DeleteObjectsRequest dor = new DeleteObjectsRequest(BUCKET_NAME).withKeys(object_keys);
            s3.deleteObjects(dor);
        } catch (AmazonServiceException e) {
        }
    }

    /**
     * 拷贝文件到指定目录
     *
     * @param fromFilePath   拷贝文件[ eg: temp/2019-08-07/xxx.jpg]
     * @param toFilePath     拷贝目标路径[ eg: xxx/xxxx ](文件夹，最后不需要斜线)
     * @param deleteTempFile 是否删除源文件[ true-删除源文件，false-保留源文件 ]
     * @return 目标文件路径
     */
    public static String copyFile(String fromFilePath, String toFilePath, boolean deleteTempFile) {

        // 目标文件
        toFilePath = toFilePath + fromFilePath.substring(fromFilePath.lastIndexOf("/") + 1);

        try {
            CopyObjectRequest request = new CopyObjectRequest(BUCKET_NAME, fromFilePath, BUCKET_NAME, toFilePath);
            request.setCannedAccessControlList(CannedAccessControlList.PublicRead);

            CopyObjectResult copyObjectResult = s3.copyObject(request);
            if (StringUtils.isNotEmpty(copyObjectResult.getETag())) {
                // 删除源对象
                if (deleteTempFile) {
                    s3.deleteObject(BUCKET_NAME, fromFilePath);
                }
                return toFilePath;
            }

        } catch (Exception e) {
            System.out.println("亚马逊复制文件失败：" + fromFilePath);
        }
        return null;
    }

    /**
     * 异步拷贝文件
     *
     * @param fromFilePath 源文件数组地址[ eg: test-file/xxx/xxx.jpg ]
     * @param toFilePath   拷贝目标路径[ eg: xxx/xxxx ](文件夹，最后不需要斜线)( 不需要写test-file 或 online-file )
     */
    public static void copyFileThread(List<String> fromFilePath, String toFilePath) {

        if (fromFilePath.size() <= 0) {
            return;
        }

        ExecutorFactory threadPool = () -> Executors.newFixedThreadPool(32);
        TransferManager transferManager = TransferManagerBuilder.standard()
                .withS3Client(s3).withExecutorFactory(threadPool)
                .build();

        fromFilePath.stream().filter(fromFileItem -> StringUtils.isNotEmpty(fromFileItem)).forEach(fromFileItem -> {
            try {
                // 要拷贝的目的文件
                String toFilePathItem = toFilePath + fromFileItem.substring(fromFileItem.lastIndexOf("/") + 1);

                CopyObjectRequest copyObjectRequest = new CopyObjectRequest(BUCKET_NAME, fromFileItem, BUCKET_NAME, toFilePathItem);

                Copy copy = transferManager.copy(copyObjectRequest, s3, null);
                // 返回一个异步结果 copy, 可同步的调用 waitForCopyResult 等待 copy 结束, 成功返回 CopyResult, 失败抛出异常.
                //CopyResult copyResult = copy.waitForCopyResult();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
    // endregion

    // region 私有方法

    /**
     * 检查文件是否为空
     *
     * @param imageFile
     * @return
     */
    private static boolean isEmpty(MultipartFile imageFile) {
        if (imageFile == null || imageFile.getSize() <= 0) {
            return true;
        }
        return false;
    }

    // endregion

    public static void main(String[] args) throws IOException {
        // 上传图片测试
        uploadFile();

//        // 删除文件
//        deleteObject("testaee4c017-1feb-42b1-8a89-f93598e6dfaf.png");
//
//        // 删除文件夹
//        deleteFolder("test", true);
//
//        // 同步拷贝一张图片
//        copyFile("test/47b.png", "test123", true);
//
//        // 异步拷贝
//        copyThread();
//
//        // 同步拷贝
//        copyTongbu();
    }

    private static void uploadFile() throws IOException {
        File file = new File("/Users/mac/Downloads/apache-jmeter-5.3.tgz");
        FileInputStream fileInputStream = new FileInputStream(file);
        MultipartFile multipartFile = new MockMultipartFile(file.getName(), file.getName(),
                ContentType.APPLICATION_OCTET_STREAM.toString(), fileInputStream);

        String filePath = updateLoadFile(multipartFile, "test004", "");
        System.out.println(filePath);
    }

    private static void copyThread() {

        Date start = new Date();
        long startM = System.currentTimeMillis();
        System.out.println("开始时间==============>" + start);

        List<String> fromFilePath = new ArrayList<>();

        for (int i = 1; i < 101; i++) {
            fromFilePath.add("source/" + i + ".png");
        }

        copyFileThread(fromFilePath, "target");
        Date end = new Date();
        System.out.println("结束时间==============>" + end + ",异步拷贝耗时：" + (System.currentTimeMillis() - startM));

    }

    private static void copyTongbu() {

        Date start = new Date();
        long startM = System.currentTimeMillis();
        System.out.println("开始时间==============>" + start);

        for (int i = 1; i < 101; i++) {
            copyFile("source/" + i + ".png", "tongbuTarget", true);
        }

        Date end = new Date();
        System.out.println("结束时间==============>" + end + ",同步拷贝耗时：" + (System.currentTimeMillis() - startM));
    }
}
