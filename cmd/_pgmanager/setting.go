package main

import (
	"fmt"

	// Packages
	httpclient "github.com/mutablelogic/go-pg/pkg/manager/httpclient"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type SettingCommands struct {
	ListSetting   ListSettingCommand   `cmd:"" name:"settings" help:"List server settings."`
	ListCategory  ListCategoryCommand  `cmd:"" name:"setting-categories" help:"List setting categories."`
	GetSetting    GetSettingCommand    `cmd:"" name:"setting" help:"Get a server setting."`
	UpdateSetting UpdateSettingCommand `cmd:"" name:"update-setting" help:"Update a server setting."`
	ResetSetting  ResetSettingCommand  `cmd:"" name:"reset-setting" help:"Reset a server setting to default."`
}

type ListSettingCommand struct {
	Category string  `name:"category" help:"Filter by category name"`
	Offset   uint64  `name:"offset" help:"Offset for pagination"`
	Limit    *uint64 `name:"limit" help:"Limit for pagination"`
}

type ListCategoryCommand struct{}

type GetSettingCommand struct {
	Name string `arg:"" name:"name" help:"Setting name"`
}

type UpdateSettingCommand struct {
	Name   string `arg:"" name:"name" help:"Setting name"`
	Value  string `arg:"" name:"value" help:"New setting value"`
	Reload bool   `name:"reload" help:"Reload configuration after update (only for sighup context settings)"`
}

type ResetSettingCommand struct {
	Name   string `arg:"" name:"name" help:"Setting name"`
	Reload bool   `name:"reload" help:"Reload configuration after reset (only for sighup context settings)"`
}

///////////////////////////////////////////////////////////////////////////////
// COMMANDS

func (cmd *ListSettingCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Build options
	opts := []httpclient.Opt{httpclient.WithOffsetLimit(cmd.Offset, cmd.Limit)}
	if cmd.Category != "" {
		opts = append(opts, httpclient.WithCategory(&cmd.Category))
	}

	// List settings
	settings, err := client.ListSettings(ctx.ctx, opts...)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(settings)
	return nil
}

func (cmd *ListCategoryCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// List setting categories
	categories, err := client.ListSettingCategories(ctx.ctx)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(categories)
	return nil
}

func (cmd *GetSettingCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Get one setting
	setting, err := client.GetSetting(ctx.ctx, cmd.Name)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(setting)
	return nil
}

func (cmd *UpdateSettingCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Build meta
	meta := schema.SettingMeta{
		Value: &cmd.Value,
	}

	// Build options
	opts := []httpclient.Opt{}
	if cmd.Reload {
		opts = append(opts, httpclient.WithReload(true))
	}

	// Update the setting
	setting, err := client.UpdateSetting(ctx.ctx, cmd.Name, meta, opts...)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(setting)
	return nil
}

func (cmd *ResetSettingCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Build meta with nil value (means reset)
	meta := schema.SettingMeta{
		Value: nil,
	}

	// Build options
	opts := []httpclient.Opt{}
	if cmd.Reload {
		opts = append(opts, httpclient.WithReload(true))
	}

	// Reset the setting
	setting, err := client.UpdateSetting(ctx.ctx, cmd.Name, meta, opts...)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(setting)
	return nil
}
