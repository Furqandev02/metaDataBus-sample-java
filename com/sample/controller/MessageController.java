package com.sample.controller;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;

public class MessageController {

    /**
     * Submit new sms.
     *
     * @param redirectAttributes   the redirect attributes
     * @param req                  the req
     * @param uploadFile           the upload file
     * @param notificationTypeId   the notification type id
     * @param manuallyAddedNumbers the numbers
     * @param topicId              the topic id
     * @param mobileScreenId       the mobile screen id
     * @param mobilePlatformId     the mobile platform id
     * @param mobileApplicationId  the mobile application id
     * @param image                the image
     * @param whatsAppFile         the whats app file
     * @param whatsAppTypes        the whats app types
     * @param whatsAppCaption      the whats app caption
     * @return the string
     */
    @RequestMapping(value = "/submit-new-sms", method = RequestMethod.POST)
    public String submitNewNotification(RedirectAttributes redirectAttributes, HttpServletRequest req,
                                        @RequestParam("uploadFile") MultipartFile uploadFile, Integer notificationTypeId,
                                        @RequestParam("manuallyAddedNumbers") String manuallyAddedNumbers, Integer topicId, Integer mobileScreenId,
                                        Integer mobilePlatformId, Integer mobileApplicationId, @RequestParam(value = "picture") MultipartFile image,
                                        @RequestParam(value = "whatsAppFile") MultipartFile whatsAppFile,
                                        @RequestParam(value = "whatsAppTypes", required = false) String whatsAppTypes,
                                        @RequestParam(value = "whatsAppCaption", required = false) String whatsAppCaption, String lat, String lng,
                                        String WhatsappTemplateId,
                                        @RequestParam(value = "whatsappTemplateText", required = false) String whatsappTemplateText) {
        SystemUser systemUser = super.getAuthUser();

        if(req.getParameter("messageTypeId") != null && !req.getParameter("messageTypeId").isEmpty()) {
            MessageType msgType = messageTypeDAO.findById(Integer.parseInt(req.getParameter("messageTypeId")));
            if(notificationTypeId.equals(Constants.NotificationType.SMS.getId())
                    && (msgType.getName().equals("رسائل دعائية") || msgType.getName().equals("رسائل توعوية"))
                    && !Utility.isSMSTimeAllowed(req.getParameter("date"))) {
                redirectAttributes.addFlashAttribute("Error", true);
                redirectAttributes.addFlashAttribute("message", resourceBundle.getString("messages.allowed.time"));
                return "redirect:/messages/create-messages";
            }
        }

        selectedListNumbersByUsers.putIfAbsent(systemUser.getId(), new ArrayList<>());
        NotificationType notificationType = notificationTypeDAO.findById(notificationTypeId);
        UploadVirtualListFileThread uploadVirtualListFileThread = new UploadVirtualListFileThread(creditService,
                operatorService, outMessagesWfaDAO, outMessagesDAO, smscConnectionUserDAO, systemUserDAO,
                countryService, emailService, broadcastMessageDAO, listNumberDAO, listDAO, readExcelFile,
                configurationFileConstants, blacklistNumberDAO);
        Enumeration<String> out = req.getParameterNames();
        BroadcastMessage broadcastMessage = new BroadcastMessage();
        broadcastMessage.setStatus(BroadcastMessageStatus.PENDING);
        broadcastMessage.setCreatedBy(systemUser);
        broadcastMessage.setCreationDate(LocalDateTime.now());
        broadcastMessage.setNotificationType(notificationType);
        // check if user had credit permission
        List<SystemUserCredit> systemUserCreditList = systemUserCreditDAO.findSystemUserCreditBySystemUserAndNotificationType(
                systemUser.getId(), notificationType.getId());
        if (systemUserCreditList != null && !systemUserCreditList.isEmpty()) {
            String text = "";
            // take interface value to save it in objects fields
            while (out.hasMoreElements()) {
                String x = out.nextElement().toString();
                if (x.equals("messageSenderId")) {
                    String messageSenderId = req.getParameter(x);
                    if (messageSenderId != null && !messageSenderId.equals("")) {
                        broadcastMessage.setMessageSender(messageSenderDAO.findById(Integer.parseInt(messageSenderId)));
                    }
                } else if (x.equals("messageTypeId")) {
                    String messageTypeId = req.getParameter(x);
                    if (messageTypeId != null && !messageTypeId.equals("")) {
                        broadcastMessage.setMessageType(messageTypeDAO.findById(Integer.parseInt(req.getParameter(x))));
                    }
                } else if (x.equals("text")) {
                    text = req.getParameter(x);
                    text = text.replaceAll("\r\n", "\n");
                    broadcastMessage.setMessagePart(Utility.getSMSPartsCount(text));
                    broadcastMessage.setText(text);
                } else if (x.equals("title")) {
                    broadcastMessage.setTitle(req.getParameter(x));
                } else if (x.equals("messagePart")) {
                    broadcastMessage.setMessagePart(Integer.parseInt(req.getParameter(x)));
                } else if (x.equals("date") && !req.getParameter(x).equals("")) {
                    broadcastMessage.setSendDate(
                            LocalDateTime.parse(req.getParameter(x), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")));
                } else if (x.equals("sendDate") && req.getParameter(x).equals("0")) {
                    broadcastMessage.setSendDate(LocalDateTime.now());
                } else if (x.equals("receiverOptions")) {
                    if (broadcastMessage.getSendDate() == null) {
                        broadcastMessage.setSendDate(LocalDateTime.now());
                    }
                    if (notificationTypeId.equals(Constants.NotificationType.PUSH_NOTIFICATION.getId())) {
                        boolean pushNotificationResult = pushNotificationHandle(mobileScreenId, image, systemUser,
                                topicId, redirectAttributes, broadcastMessage, mobileApplicationId, mobilePlatformId);
                        if (!pushNotificationResult) {
                            return "redirect:/messages/create-messages";
                        }
                    } else {
                        // if notification type => SMS, whatsApp, Email
                        // whatsApp only
                        if (notificationTypeId.equals(Constants.NotificationType.WHATSAPP.getId())) {
                            // if notification type => whatsApp
                            boolean whatsAppResult = whatsAppService.whatsappHandle(whatsAppFile, broadcastMessage,
                                    null, whatsAppTypes, whatsAppCaption, systemUser, redirectAttributes, lat, lng,
                                    null, (WhatsappTemplateId != null ? WhatsappTemplateId : null),
                                    (WhatsappTemplateId != null ? whatsappTemplateText : null), resourceBundle);
                            if (!whatsAppResult) {
                                return "redirect:/messages/create-messages";
                            }
                        }
                        if ((manuallyAddedNumbers != null && !manuallyAddedNumbers.isEmpty())
                                || !selectedListNumbersByUsers.get(systemUser.getId()).isEmpty()
                                || !uploadFile.isEmpty()) {
                            ListEntity addList = null;
                            if (broadcastMessage.getSendDate().isAfter(LocalDateTime.now())) {
                                addList = new ListEntity();
                                addList.setName(broadcastMessage.getTitle());
                                addList.setStatus(ListStatus.VIRTUAL);
                                addList.setCreatedBy(systemUser);
                                addList.setCreationDate(LocalDateTime.now());
                                addList.setTotalNumber(0);
                                listDAO.add(addList);
                                broadcastMessage.setList(addList);
                            }
                            Path path = Paths.get(configurationFileConstants.uploadedVirtualListNumbersFilePath);
                            try {
                                if (!Files.exists(path)) {
                                    Files.createDirectories(path);
                                }
                            } catch (IOException e) {
                                LOGGER.error("Failed to create directory [" + configurationFileConstants.uploadedVirtualListNumbersFilePath + "]", e);
                            }
                            if ((manuallyAddedNumbers != null && !manuallyAddedNumbers.isEmpty())
                                    && !selectedListNumbersByUsers.get(systemUser.getId()).isEmpty()
                                    && !uploadFile.isEmpty()) {
                                LOGGER.info("Adding new broadcast message by choosing from existing lists, uploading excel sheet, and adding number(s) manually");
                                try {
                                    File tempFile = new File(
                                            configurationFileConstants.uploadedVirtualListNumbersFilePath + uploadFile
                                                    .getOriginalFilename().replace(".xlsx", "_" + systemUser.getId()
                                                            + "_" + new Date().getTime() + ".xlsx"));
                                    FileUtils.writeByteArrayToFile(tempFile, uploadFile.getBytes());
                                    selectedListNumbersByUsers.get(systemUser.getId())
                                            .addAll(getManuallyAddedListNumbers(manuallyAddedNumbers, addList));
                                    uploadVirtualListFileThread.initThread(addList, systemUser, tempFile,
                                            broadcastMessage, notificationType, systemUserCreditList, resourceBundle,
                                            selectedListNumbersByUsers.get(systemUser.getId()));
                                } catch (Exception e) {
                                    LOGGER.error("Failed to process uploaded file", e);
                                    redirectAttributes.addFlashAttribute("Error", true);
                                    redirectAttributes.addFlashAttribute("message", resourceBundle.getString("general.unexpectedError"));
                                }
                            } else if (!selectedListNumbersByUsers.get(systemUser.getId()).isEmpty()
                                    && !uploadFile.isEmpty()) {
                                LOGGER.info(
                                        "Adding new brodcast message by uploading excel sheet and adding number(s) manually");
                                try {
                                    File tempFile = new File(configurationFileConstants.uploadedVirtualListNumbersFilePath + uploadFile
                                                    .getOriginalFilename().replace(".xlsx", "_" + systemUser.getId() + "_" + new Date().getTime() + ".xlsx"));
                                    FileUtils.writeByteArrayToFile(tempFile, uploadFile.getBytes());
                                    uploadVirtualListFileThread.initThread(addList, systemUser, tempFile,
                                            broadcastMessage, notificationType, systemUserCreditList, resourceBundle,
                                            selectedListNumbersByUsers.get(systemUser.getId()));
                                } catch (Exception e) {
                                    LOGGER.error("Failed to process uploaded file", e);
                                    redirectAttributes.addFlashAttribute("Error", true);
                                    redirectAttributes.addFlashAttribute("message", resourceBundle.getString("general.unexpectedError"));
                                }
                            } else if ((manuallyAddedNumbers != null && !manuallyAddedNumbers.isEmpty())
                                    && !selectedListNumbersByUsers.get(systemUser.getId()).isEmpty()) {
                                LOGGER.info("Adding new broadcast message by choosing from existing lists and adding number(s) manually");
                                selectedListNumbersByUsers.get(systemUser.getId())
                                        .addAll(getManuallyAddedListNumbers(manuallyAddedNumbers, addList));
                                uploadVirtualListFileThread.initThread(addList, systemUser, null, broadcastMessage,
                                        notificationType, systemUserCreditList, resourceBundle,
                                        selectedListNumbersByUsers.get(systemUser.getId()));
                            } else if ((manuallyAddedNumbers != null && !manuallyAddedNumbers.isEmpty())
                                    && !uploadFile.isEmpty()) {
                                LOGGER.info("Adding new brodcast message by uploading excel sheet and adding number(s) manually");
                                try {
                                    File tempFile = new File(
                                            configurationFileConstants.uploadedVirtualListNumbersFilePath + uploadFile
                                                    .getOriginalFilename().replace(".xlsx", "_" + systemUser.getId()
                                                            + "_" + new Date().getTime() + ".xlsx"));
                                    FileUtils.writeByteArrayToFile(tempFile, uploadFile.getBytes());
                                    uploadVirtualListFileThread.initThread(addList, systemUser, tempFile,
                                            broadcastMessage, notificationType, systemUserCreditList, resourceBundle,
                                            getManuallyAddedListNumbers(manuallyAddedNumbers, addList));
                                } catch (Exception e) {
                                    LOGGER.error("Failed to process uploaded file", e);
                                    redirectAttributes.addFlashAttribute("Error", true);
                                    redirectAttributes.addFlashAttribute("message", resourceBundle.getString("general.unexpectedError"));
                                }
                            } else if (!selectedListNumbersByUsers.get(systemUser.getId()).isEmpty()) {
                                LOGGER.info("Adding new broadcast message by choosing from existing lists");
                                uploadVirtualListFileThread.initThread(addList, systemUser, null, broadcastMessage,
                                        notificationType, systemUserCreditList, resourceBundle,
                                        selectedListNumbersByUsers.get(systemUser.getId()));
                            } else if (!uploadFile.isEmpty()) {
                                LOGGER.info("Adding new brodcast message by uploading excel sheet");
                                try {
                                    File tempFile = new File(
                                            configurationFileConstants.uploadedVirtualListNumbersFilePath + uploadFile
                                                    .getOriginalFilename().replace(".xlsx", "_" + systemUser.getId() + "_" + new Date().getTime() + ".xlsx"));
                                    FileUtils.writeByteArrayToFile(tempFile, uploadFile.getBytes());
                                    uploadVirtualListFileThread.initThread(addList, systemUser, tempFile,
                                            broadcastMessage, notificationType, systemUserCreditList, resourceBundle, null);
                                } catch (Exception e) {
                                    LOGGER.error("Failed to process uploaded file", e);
                                    redirectAttributes.addFlashAttribute("Error", true);
                                    redirectAttributes.addFlashAttribute("message",
                                            resourceBundle.getString("general.unexpectedError"));
                                }
                            } else if (manuallyAddedNumbers != null && !manuallyAddedNumbers.isEmpty()) {
                                LOGGER.info("Adding new broadcast message by adding number manually");
                                uploadVirtualListFileThread.initThread(addList, systemUser, null, broadcastMessage,
                                        notificationType, systemUserCreditList, resourceBundle,
                                        getManuallyAddedListNumbers(manuallyAddedNumbers, addList));
                            }
                            Thread runner = new Thread(uploadVirtualListFileThread);
                            runner.start();
                            redirectAttributes.addFlashAttribute("wait", true);
                            if (broadcastMessage.getSendDate().isAfter(LocalDateTime.now())) {
                                redirectAttributes.addFlashAttribute("message", resourceBundle.getString("general.waitingSchedule"));
                            } else {
                                redirectAttributes.addFlashAttribute("message", resourceBundle.getString("general.waiting"));
                            }
                        } else {
                            redirectAttributes.addFlashAttribute("Error", true);
                            redirectAttributes.addFlashAttribute("message", resourceBundle.getString("list.zeroSelectedListNumbers"));
                        }
                    }
                }
            }
        } else {
            redirectAttributes.addFlashAttribute("Error", true);
            redirectAttributes.addFlashAttribute("message", resourceBundle.getString("pageContent.user-credit.notificationTypeInsufficientCredit"));
        }
        redirectAttributes.addFlashAttribute("isSendToGroup", false);
        return "redirect:/messages/create-messages";
    }

}
