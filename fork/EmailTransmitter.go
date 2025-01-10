package fork

import (
	"errors"
	"fmt"
	"gopkg.in/gomail.v2"
)

type EmailTransmitter struct {
	Sender map[string]SenderInfo
}

var emailTransmitterInstance EmailTransmitter

func GetInstanceByEmailTransmitter() *EmailTransmitter {
	return &emailTransmitterInstance
}

func (that *EmailTransmitter) InitSender(alias, sender, pwd, addr string, port int) {
	if nil == that.Sender {
		that.Sender = make(map[string]SenderInfo)
	}
	s := SenderInfo{
		Alias:     alias,
		Sender:    sender,
		SPassword: pwd,
		SMTPAddr:  addr,
		SMTPPort:  port,
	}
	that.Sender[alias] = s
}

type SenderInfo struct {
	// 别名
	Alias string
	// 发件人账号
	Sender string
	// 发件人密码
	SPassword string
	// SMTP 服务器地址， Q
	SMTPAddr string
	// SMTP端口
	SMTPPort int
}

type EmailInfo struct {
	// 邮件标题
	Title string
	//	邮件内容类型
	ContentType string
	// 邮件内容
	Body string
	// 收件人列表
	RecipientList []string
}

// SendEmail 发送邮件
// senderAlias string 发送者
func (that *EmailTransmitter) SendEmail(senderAlias string, info EmailInfo) error {
	sender, isOk := that.Sender[senderAlias]
	if !isOk {
		return errors.New(fmt.Sprintf("not found %s", senderAlias))
	}
	m := gomail.NewMessage()
	m.SetHeader("From", sender.Sender, sender.Alias)
	m.SetHeader("To", info.RecipientList...)
	m.SetHeader("Subject", info.Title)
	m.SetBody(info.ContentType, info.Body)
	err := gomail.NewDialer(sender.SMTPAddr, sender.SMTPPort, sender.Sender, sender.SPassword).DialAndSend(m)
	if nil != err {
		return err
	}
	return nil
}
