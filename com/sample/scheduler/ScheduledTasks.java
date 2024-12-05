package com.sample.scheduler;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ScheduledTasks {

    /**
     * Handle notification messages.
     */
    @Scheduled(cron = "*/15 * * * * *")
    @SchedulerLock(name = "ScheduledTasks_fetchIntoOutMessages", lockAtLeastForString = "PT14S", lockAtMostForString = "PT1M")
    public void handleNotificationMessages() {
        List<BroadcastMessage> broadcastMessagesList = broadcastMessageDAO.findAllByStatusAndDescPriorityId(BroadcastMessageStatus.PENDING);
        if (broadcastMessagesList != null && !broadcastMessagesList.isEmpty()) {
            LOGGER.info("Start handling pendingBroadcastMessages ...");
            for (BroadcastMessage broadcastMessage : broadcastMessagesList) {
                switch (broadcastMessage.getNotificationType().getId()) {
                    case 1:
                        handleSMSNotification(broadcastMessage);
                        break;
                    case 2:
                        handleEmailNotification(broadcastMessage);
                        break;
                    case 3:
                        handleWhatsAppNotification(broadcastMessage);
                        break;
                    case 4:
                        handlePushNotification(broadcastMessage);
                        break;
                    default:
                        LOGGER.info("Unknown notification type " + broadcastMessage.getNotificationType().getId());
                        break;
                }
            }
            LOGGER.info("Finish handling [" + broadcastMessagesList.size() + "] pendingBroadcastMessages ...");
        }
    }

    /**
     * This job is responsible to handle pending broadcast messages, where it fetch the list
     * numbers per each broadcast message list and populate them into out messages, where if
     * success, the broadcast message status will be set to "BroadcastMessageStatus.SENT" and
     * its systemUserCreditLog(s) transaction type will be set to "TransactionType.SENT". For
     * Virtual type lists, their list numbers will be deleted as to avoid records duplication.
     *
     * @param broadcastMessage
     *        the broadcast message
     *
     * @author Abdullah
     * @created Feb 19, 2020
     */
    private void handleSMSNotification(BroadcastMessage broadcastMessage) {
        LOGGER.info("Start handling SMS pendingBroadcastMessage with id [" + broadcastMessage.getId() + "]");
        List<ListNumber> broadcastMessageListNumbers = listNumberDAO.findAllActiveListNumbersByList(broadcastMessage.getList().getId());

        SystemUser createdBySystemUser = broadcastMessage.getCreatedBy();

        List<SmscConnectionUser> systemUserSmscConnectionsList = smscConnectionUserDAO.findSmscConnectionsListBySystemUser(createdBySystemUser.getId());
        HashMap<Integer, SmscConnection> systemUserCountriesSmscs = new HashMap<Integer, SmscConnection>();

        for (SmscConnectionUser smscConnectionUser : systemUserSmscConnectionsList) {
            systemUserCountriesSmscs.put(smscConnectionUser.getCountry().getId(), smscConnectionUser.getSmscConnection());
        }
        HashMap<Integer, SmscConnection> systemUserOperatorsSmscs = new HashMap<Integer, SmscConnection>();
        for (SmscConnectionUser smscConnectionUser : systemUserSmscConnectionsList) {
            systemUserOperatorsSmscs.put(smscConnectionUser.getOperator() != null ? smscConnectionUser.getOperator().getId() : null,
                    smscConnectionUser.getSmscConnection());
        }
        List<OutMessages> outMessagesListToAdd = new ArrayList<OutMessages>();

        String hasRoleForHLR = systemUserService.getAllSystemUserRoles(broadcastMessage.getCreatedBy().getId()).stream().map(x -> x.getScreenFunction().getName())
                .filter(x -> x.equalsIgnoreCase("enable_hlr")).findFirst().orElse(null);

        if(hasRoleForHLR != null) {
            fgcService.checkBulkMsisdnOperator(broadcastMessageListNumbers);
        }

        for (ListNumber listNumber : broadcastMessageListNumbers) {
            Country msisdnCountry = listNumber.getCountry();
            SmscConnection msisdnSmsc = null;
            Operator msisdnOperator = operatorService.checkOperatorForBroadcast(listNumber.getMsisdn(), msisdnCountry);
            if (systemUserSmscConnectionsList != null && !systemUserSmscConnectionsList.isEmpty()) {
                if (msisdnOperator != null) {
                    msisdnSmsc = systemUserOperatorsSmscs.get(msisdnOperator.getId());
                } else {
                    msisdnSmsc = systemUserCountriesSmscs.get(msisdnCountry.getId());
                }
            }
            if (msisdnSmsc == null && msisdnOperator != null) {
                msisdnSmsc = operatorService.getOperatorsSmscs().get(msisdnOperator.getId());
            }

            if (msisdnSmsc == null) {
                msisdnSmsc = countryService.getCountriesSmscs().get(msisdnCountry.getId());
            }

            if (msisdnSmsc != null) {
                StringBuffer messageText = new StringBuffer(broadcastMessage.getText());
                String predefinedVariable = listNumber.getPredefinedVariable();
                replaceVariablesInText(messageText, predefinedVariable);
                OutMessages outMessage = new OutMessages(msisdnCountry, broadcastMessage.getMessageSender().getName(), listNumber.getMsisdn(),
                        messageText.toString(), LocalDateTime.now(), null, null, broadcastMessage, createdBySystemUser.getMessagePriority(), createdBySystemUser,
                        msisdnSmsc, LocalDateTime.now(), broadcastMessage.getNotificationType(), broadcastMessage.getExtraParams(), 0);
                outMessage.setFlag(OutMessageFlag.PENDING);
                outMessage.setMessageType(broadcastMessage.getMessageType());
                outMessagesListToAdd.add(outMessage);
            } else {
                LOGGER.error("No SmscConnectionUser is defined for country id [" + msisdnCountry.getId() + "]");
            }
        }
        try {
            if (!outMessagesListToAdd.isEmpty()) {
                boolean isAddMessagesSuccess = outMessagesDAO.addAll(outMessagesListToAdd);
                if (isAddMessagesSuccess) {
                    finalizeBroadcastMessageHandling(broadcastMessage);
                    // if this broadcast message list status is virtual, delete its list numbers
                    if (broadcastMessage.getList().getStatus().equals(ListStatus.VIRTUAL)) {
                        for (ListNumber listNumber : broadcastMessageListNumbers) {
                            listNumberDAO.deleteById(listNumber.getId());
                        }
                    }
                }
            } else {
                LOGGER.warn("There are no SMS messages to handle");
                broadcastMessage.setStatus(BroadcastMessageStatus.NOT_SENT);
                broadcastMessageDAO.update(broadcastMessage);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to add list of out messages to DB using broadcastMessage id [" + broadcastMessage.getId() + "]", e);
            return;
        }
        LOGGER.info("Finish handling SMS pendingBroadcastMessage with id [" + broadcastMessage.getId() + "]");
    }


    /**
     * Send pending messages to ActiveMQ and archive them
     *
     * @author Rami
     * @created Mar 01, 2020
     */
    @Scheduled(cron = "*/1 * * * * *")
    @SchedulerLock(name = "ScheduledTasks_sendOutMessages", lockAtMostForString = "PT1M")
    public void sendMessage() {
        List<OutMessages> outMessagesList = new ArrayList<OutMessages>();
        // synchronized (ScheduledTasks.class) {
        List<Integer> outMessageIdsList = new ArrayList<Integer>();
        outMessagesList = outMessagesDAO.getReadyToPublishMessages();
        if (outMessagesList != null && !outMessagesList.isEmpty()) {

            outMessagesList = checkUrlWhitelistAndKeyword(outMessagesList);


            sendMessageThread.initThread(outMessagesList);
            Thread thread = new Thread(sendMessageThread);
            thread.start();
            List<OutMessagesHistory> outMessagesHistoryList = new ArrayList<OutMessagesHistory>();
            for (OutMessages outMessage : outMessagesList) {
                OutMessagesHistory outMessagesHistory = new OutMessagesHistory(outMessage.getId(), outMessage.getCountry().getId(), outMessage.getHeader(),
                        outMessage.getMsisdn(), outMessage.getText(), LocalDateTime.now(), outMessage.getStatusId(), outMessage.getStatusDescription(),
                        outMessage.getBroadcastMessage() != null ? outMessage.getBroadcastMessage().getId() : null,
                        outMessage.getMessagePriority() != null ? outMessage.getMessagePriority().getId() : null, outMessage.getSystemUser().getId(),
                        outMessage.getSmscConnection() != null ? outMessage.getSmscConnection().getId().toString() : null, outMessage.getSendDate(),
                        outMessage.getNotificationType(), outMessage.getExtraParams(), outMessage.getMessageType());
                outMessagesHistoryList.add(outMessagesHistory);
                outMessageIdsList.add(outMessage.getId());
            }
            // start archiving out messages
            if (outMessagesHistoryList .size() > 0 && outMessagesHistoryDAO.addAll(outMessagesHistoryList)) {
                outMessagesDAO.deleteAll(outMessageIdsList);
            }
        }
        // }
    }


}
