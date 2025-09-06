package com.basic.service;

import com.basic.commons.vo.UploadFileModel;
import com.basic.commons.vo.UploadItem;

/**
 * AbstractThirdUploadFileTemplateService <br>
 *
 * @author: lidaye <br>
 * @date: 2021/11/12  <br>
 */
public abstract class AbstractThirdUploadFileTemplateService extends AbstractUploadFileTemplateService implements ThirdUploadFileTemplateService{


    @Override
    public UploadItem doSaveFileStore(UploadFileModel model){
       return saveFile(model);
    }
}
