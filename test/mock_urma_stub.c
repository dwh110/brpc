#include <ub/umdk/urma/urma_api.h>

urma_status_t urma_init(urma_init_attr_t *conf)
{
    return 0;
}

urma_status_t urma_uninit(void)
{
    return 0;
}

urma_device_t **urma_get_device_list(int *num_devices)
{
    return NULL;
}

void urma_free_device_list(urma_device_t **device_list)
{
}

urma_eid_info_t *urma_get_eid_list(urma_device_t *dev, uint32_t *cnt)
{
    return NULL;
}

void urma_free_eid_list(urma_eid_info_t *eid_list)
{
}

urma_device_t *urma_get_device_by_name(char *dev_name)
{
    return NULL;
}

urma_device_t *urma_get_device_by_eid(urma_eid_t eid, urma_transport_type_t type)
{
    return NULL;
}

urma_status_t urma_query_device(urma_device_t *dev, urma_device_attr_t *dev_attr)
{
    return 0;
}

urma_context_t *urma_create_context(urma_device_t *dev, uint32_t eid_index)
{
    return NULL;
}

urma_status_t urma_delete_context(urma_context_t *ctx)
{
    return 0;
}

urma_status_t urma_set_context_opt(urma_context_t *ctx, urma_opt_name_t opt_name, const void *opt_value,
                                   size_t opt_len)
{
    return 0;
}

urma_jfc_t *urma_create_jfc(urma_context_t *ctx, urma_jfc_cfg_t *jfc_cfg)
{
    return NULL;
}

urma_status_t urma_modify_jfc(urma_jfc_t *jfc, urma_jfc_attr_t *attr)
{
    return 0;
}

urma_status_t urma_delete_jfc(urma_jfc_t *jfc)
{
    return 0;
}

urma_status_t urma_alloc_jfc(urma_context_t *urma_ctx, urma_jfc_cfg_t *cfg, urma_jfc_t **jfc)
{
    return 0;
}

urma_status_t urma_set_jfc_opt(urma_jfc_t *jfc, uint64_t opt, void *buf, uint32_t len)
{
    return 0;
}

urma_status_t urma_active_jfc(urma_jfc_t *jfc)
{
    return 0;
}

urma_status_t urma_get_jfc_opt(urma_jfc_t *jfc, uint64_t opt, void *buf, uint32_t len)
{
    return 0;
}

urma_status_t urma_deactive_jfc(urma_jfc_t *jfc)
{
    return 0;
}

urma_status_t urma_free_jfc(urma_jfc_t *jfc)
{
    return 0;
}

urma_status_t urma_delete_jfc_batch(urma_jfc_t **jfc_arr, int jfc_num, urma_jfc_t **bad_jfc)
{
    return 0;
}

urma_jfs_t *urma_create_jfs(urma_context_t *ctx, urma_jfs_cfg_t *jfs_cfg)
{
    return NULL;
}

urma_status_t urma_modify_jfs(urma_jfs_t *jfs, urma_jfs_attr_t *attr)
{
    return 0;
}

urma_status_t urma_query_jfs(urma_jfs_t *jfs, urma_jfs_cfg_t *cfg, urma_jfs_attr_t *attr)
{
    return 0;
}

urma_status_t urma_delete_jfs(urma_jfs_t *jfs)
{
    return 0;
}

urma_status_t urma_delete_jfs_batch(urma_jfs_t **jfs_arr, int jfs_num, urma_jfs_t **bad_jfs)
{
    return 0;
}

int urma_flush_jfs(urma_jfs_t *jfs, int cr_cnt, urma_cr_t *cr)
{
    return 0;
}

urma_status_t urma_alloc_jfs(urma_context_t *urma_ctx, urma_jfs_cfg_t *cfg, urma_jfs_t **jfs)
{
    return 0;
}

urma_status_t urma_set_jfs_opt(urma_jfs_t *jfs, uint64_t opt, void *buf, uint32_t len)
{
    return 0;
}

urma_status_t urma_active_jfs(urma_jfs_t *jfs)
{
    return 0;
}

urma_status_t urma_get_jfs_opt(urma_jfs_t *jfs, uint64_t opt, void *buf, uint32_t len)
{
    return 0;
}

urma_status_t urma_deactive_jfs(urma_jfs_t *jfs)
{
    return 0;
}

urma_status_t urma_free_jfs(urma_jfs_t *jfs)
{
    return 0;
}

urma_jfr_t *urma_create_jfr(urma_context_t *ctx, urma_jfr_cfg_t *jfr_cfg)
{
    return NULL;
}

urma_status_t urma_modify_jfr(urma_jfr_t *jfr, urma_jfr_attr_t *attr)
{
    return 0;
}

urma_status_t urma_query_jfr(urma_jfr_t *jfr, urma_jfr_cfg_t *cfg, urma_jfr_attr_t *attr)
{
    return 0;
}

urma_status_t urma_delete_jfr(urma_jfr_t *jfr)
{
    return 0;
}

urma_status_t urma_delete_jfr_batch(urma_jfr_t **jfr_arr, int jfr_num, urma_jfr_t **bad_jfr)
{
    return 0;
}

urma_target_jetty_t *urma_import_jfr(urma_context_t *ctx, urma_rjfr_t *rjfr, urma_token_t *token_value)
{
    return NULL;
}

urma_target_jetty_t *urma_import_jfr_ex(urma_context_t *ctx, urma_rjfr_t *rjfr, urma_token_t *token_value,
    urma_import_jfr_ex_cfg_t *cfg)
{
    return NULL;
}

urma_status_t urma_unimport_jfr(urma_target_jetty_t *target_jfr)
{
    return 0;
}

urma_status_t urma_advise_jfr(urma_jfs_t *jfs, urma_target_jetty_t *tjfr)
{
    return 0;
}

urma_status_t urma_advise_jfr_async(urma_jfs_t *jfs, urma_target_jetty_t *tjfr, urma_advise_async_cb_func cb_fun,
                                    void *cb_arg)
{
    return 0;
}

urma_status_t urma_unadvise_jfr(urma_jfs_t *jfs, urma_target_jetty_t *tjfr)
{
    return 0;
}

urma_status_t urma_alloc_jfr(urma_context_t *urma_ctx, urma_jfr_cfg_t *cfg, urma_jfr_t **jfr)
{
    return 0;
}

urma_status_t urma_set_jfr_opt(urma_jfr_t *jfr, uint64_t opt, void *buf, uint32_t len)
{
    return 0;
}

urma_status_t urma_active_jfr(urma_jfr_t *jfr)
{
    return 0;
}

urma_status_t urma_get_jfr_opt(urma_jfr_t *jfr, uint64_t opt, void *buf, uint32_t len)
{
    return 0;
}

urma_status_t urma_deactive_jfr(urma_jfr_t *jfr)
{
    return 0;
}

urma_status_t urma_free_jfr(urma_jfr_t *jfr)
{
    return 0;
}

urma_jetty_t *urma_create_jetty(urma_context_t *ctx, urma_jetty_cfg_t *jetty_cfg)
{
    return NULL;
}

urma_status_t urma_modify_jetty(urma_jetty_t *jetty, urma_jetty_attr_t *attr)
{
    return 0;
}

urma_status_t urma_query_jetty(urma_jetty_t *jetty, urma_jetty_cfg_t *cfg, urma_jetty_attr_t *attr)
{
    return 0;
}

urma_status_t urma_delete_jetty(urma_jetty_t *jetty)
{
    return 0;
}

urma_status_t urma_delete_jetty_batch(urma_jetty_t **jetty_arr, int jetty_num, urma_jetty_t **bad_jetty)
{
    return 0;
}

urma_target_jetty_t *urma_import_jetty(urma_context_t *ctx, urma_rjetty_t *rjetty, urma_token_t *token_value)
{
    return NULL;
}

urma_target_jetty_t *urma_import_jetty_ex(urma_context_t *ctx, urma_rjetty_t *rjetty, urma_token_t *token_value,
                                          urma_import_jetty_ex_cfg_t *cfg)
{
    return NULL;
}

urma_status_t urma_unimport_jetty(urma_target_jetty_t *tjetty)
{
    return 0;
}

urma_status_t urma_advise_jetty(urma_jetty_t *jetty, urma_target_jetty_t *tjetty)
{
    return 0;
}

urma_status_t urma_unadvise_jetty(urma_jetty_t *jetty, urma_target_jetty_t *tjetty)
{
    return 0;
}

urma_status_t urma_bind_jetty(urma_jetty_t *jetty, urma_target_jetty_t *tjetty)
{
    return 0;
}

urma_status_t urma_bind_jetty_ex(urma_jetty_t *jetty, urma_target_jetty_t *tjetty, urma_bind_jetty_ex_cfg_t *cfg)
{
    return 0;
}

urma_status_t urma_unbind_jetty(urma_jetty_t *jetty)
{
    return 0;
}

int urma_flush_jetty(urma_jetty_t *jetty, int cr_cnt, urma_cr_t *cr)
{
    return 0;
}

urma_status_t urma_get_rjetty(urma_jetty_t *jetty, urma_rjetty_t **rjetty, uint32_t *length)
{
    return 0;
}

void urma_put_rjetty(urma_rjetty_t *rjetty)
{
}

urma_target_jetty_t *urma_import_jetty_async(urma_notifier_t *notifier, const urma_rjetty_t *rjetty,
    const urma_token_t *token_value, uint64_t user_ctx, int timeout)
{
    return NULL;
}

urma_status_t urma_unimport_jetty_async(urma_target_jetty_t *tjetty)
{
    return 0;
}

urma_status_t urma_bind_jetty_async(urma_notifier_t *notifier, urma_jetty_t *jetty, urma_target_jetty_t *tjetty,
                                    uint64_t user_ctx, int timeout)
{
    return 0;
}

urma_status_t urma_unbind_jetty_async(urma_jetty_t *jetty)
{
    return 0;
}

urma_notifier_t *urma_create_notifier(urma_context_t *ctx)
{
    return NULL;
}

urma_status_t urma_delete_notifier(urma_notifier_t *notifier)
{
    return 0;
}

urma_status_t urma_alloc_jetty(urma_context_t *urma_ctx, urma_jetty_cfg_t *cfg, urma_jetty_t **jetty)
{
    return 0;
}

urma_status_t urma_set_jetty_opt(urma_jetty_t *jetty, uint64_t opt, void *buf, uint32_t len)
{
    return 0;
}

urma_status_t urma_active_jetty(urma_jetty_t *jetty)
{
    return 0;
}

urma_status_t urma_get_jetty_opt(urma_jetty_t *jetty, uint64_t opt, void *buf, uint32_t len)
{
    return 0;
}

urma_status_t urma_deactive_jetty(urma_jetty_t *jetty)
{
    return 0;
}

urma_status_t urma_free_jetty(urma_jetty_t *jetty)
{
    return 0;
}

int urma_wait_notify(urma_notifier_t *notifier, uint32_t cnt, urma_notify_t *notify, int timeout)
{
    return 0;
}

urma_status_t urma_ack_notify(urma_context_t *ctx, uint32_t cnt, urma_notify_t *notify)
{
    return 0;
}

urma_jetty_grp_t *urma_create_jetty_grp(urma_context_t *ctx, urma_jetty_grp_cfg_t *cfg)
{
    return NULL;
}

urma_status_t urma_delete_jetty_grp(urma_jetty_grp_t *jetty_grp)
{
    return 0;
}

urma_jfce_t *urma_create_jfce(urma_context_t *ctx)
{
    return NULL;
}

urma_status_t urma_delete_jfce(urma_jfce_t *jfce)
{
    return 0;
}

urma_status_t urma_get_async_event(urma_context_t *ctx, urma_async_event_t *event)
{
    return 0;
}

void urma_ack_async_event(urma_async_event_t *event)
{
}

urma_token_id_t *urma_alloc_token_id(urma_context_t *ctx)
{
    return NULL;
}

urma_token_id_t *urma_alloc_token_id_ex(urma_context_t *ctx, urma_token_id_flag_t flag)
{
    return NULL;
}

urma_status_t urma_free_token_id(urma_token_id_t *token_id)
{
    return 0;
}

urma_target_seg_t *urma_register_seg(urma_context_t *ctx, urma_seg_cfg_t *seg_cfg)
{
    return NULL;
}

urma_status_t urma_unregister_seg(urma_target_seg_t *target_seg)
{
    return 0;
}

urma_target_seg_t *urma_import_seg(urma_context_t *ctx, urma_seg_t *seg, urma_token_t *token_value, uint64_t addr,
                                   urma_import_seg_flag_t flag)
{
    return NULL;
}

urma_status_t urma_unimport_seg(urma_target_seg_t *tseg)
{
    return 0;
}

urma_status_t urma_get_seg_ctx(urma_target_seg_t *tseg, urma_seg_t **seg, uint32_t *size)
{
    return 0;
}

void urma_put_seg_ctx(urma_seg_t *seg)
{
}

urma_status_t urma_post_jfs_wr(urma_jfs_t *jfs, urma_jfs_wr_t *wr, urma_jfs_wr_t **bad_wr)
{
    return 0;
}

urma_status_t urma_post_jfr_wr(urma_jfr_t *jfr, urma_jfr_wr_t *wr, urma_jfr_wr_t **bad_wr)
{
    return 0;
}

urma_status_t urma_post_jetty_send_wr(urma_jetty_t *jetty, urma_jfs_wr_t *wr, urma_jfs_wr_t **bad_wr)
{
    return 0;
}

urma_status_t urma_post_jetty_recv_wr(urma_jetty_t *jetty, urma_jfr_wr_t *wr, urma_jfr_wr_t **bad_wr)
{
    return 0;
}

urma_status_t urma_write(urma_jfs_t *jfs, urma_target_jetty_t *target_jfr, urma_target_seg_t *dst_tseg,
                         urma_target_seg_t *src_tseg, uint64_t dst, uint64_t src, uint32_t len, urma_jfs_wr_flag_t flag,
                         uint64_t user_ctx)
{
    return 0;
}

urma_status_t urma_read(urma_jfs_t *jfs, urma_target_jetty_t *target_jfr, urma_target_seg_t *dst_tseg,
                        urma_target_seg_t *src_tseg, uint64_t dst, uint64_t src, uint32_t len, urma_jfs_wr_flag_t flag,
                        uint64_t user_ctx)
{
    return 0;
}

urma_status_t urma_send(urma_jfs_t *jfs, urma_target_jetty_t *target_jfr, urma_target_seg_t *src_tseg, uint64_t src,
                        uint32_t len, urma_jfs_wr_flag_t flag, uint64_t user_ctx)
{
    return 0;
}

urma_status_t urma_recv(urma_jfr_t *jfr, urma_target_seg_t *recv_tseg, uint64_t buf, uint32_t len, uint64_t user_ctx)
{
    return 0;
}

int urma_poll_jfc(urma_jfc_t *jfc, int cr_cnt, urma_cr_t *cr)
{
    return 0;
}

urma_status_t urma_rearm_jfc(urma_jfc_t *jfc, bool solicited_only)
{
    return 0;
}

int urma_wait_jfc(urma_jfce_t *jfce, uint32_t jfc_cnt, int time_out, urma_jfc_t *jfc[])
{
    return 0;
}

void urma_ack_jfc(urma_jfc_t *jfc[], uint32_t nevents[], uint32_t jfc_cnt)
{
}

urma_status_t urma_get_uasid(uint32_t *uasid)
{
    return 0;
}

urma_status_t urma_user_ctl(urma_context_t *ctx, urma_user_ctl_in_t *in, urma_user_ctl_out_t *out)
{
    return 0;
}

urma_status_t urma_register_log_func(urma_log_cb_t func)
{
    return 0;
}

urma_status_t urma_register_loc_log_func(urma_loc_log_cb func)
{
    return 0;
}

urma_status_t urma_unregister_log_func(void)
{
    return 0;
}

urma_vlog_level_t urma_log_get_level(void)
{
    return 0;
}

void urma_log_set_level(urma_vlog_level_t level)
{
}

const char *urma_log_get_thread_tag(void)
{
    return NULL;
}

void urma_log_set_thread_tag(const char *tag)
{
}

int urma_get_tpn(urma_jetty_t *jetty)
{
    return 0;
}

urma_net_addr_info_t *urma_get_net_addr_list(urma_context_t *ctx, uint32_t *cnt)
{
    return NULL;
}

void urma_free_net_addr_list(urma_net_addr_info_t *net_addr_list)
{
}

int urma_modify_tp(urma_context_t *ctx, uint32_t tpn, urma_tp_cfg_t *cfg, urma_tp_attr_t *attr,
    urma_tp_attr_mask_t mask)
{
    return 0;
}

urma_status_t urma_get_tp_list(urma_context_t *ctx, urma_get_tp_cfg_t *cfg, uint32_t *tp_cnt, urma_tp_info_t *tp_list)
{
    return 0;
}

urma_status_t urma_set_tp_attr(const urma_context_t *ctx, const uint64_t tp_handle, const uint8_t tp_attr_cnt,
                               const uint32_t tp_attr_bitmap, const urma_tp_attr_value_t *tp_attr)
{
    return 0;
}

urma_status_t urma_get_tp_attr(const urma_context_t *ctx, const uint64_t tp_handle, uint8_t *tp_attr_cnt,
                               uint32_t *tp_attr_bitmap, urma_tp_attr_value_t *tp_attr)
{
    return 0;
}

urma_status_t urma_get_eid_by_ip(const urma_context_t *ctx, const urma_net_addr_t *net_addr, urma_eid_t *eid)
{
    return 0;
}

urma_status_t urma_get_ip_by_eid(const urma_context_t *ctx, const urma_eid_t *eid, urma_net_addr_t *net_addr)
{
    return 0;
}

urma_status_t urma_get_smac(const urma_context_t *ctx, uint8_t *mac)
{
    return 0;
}

urma_status_t urma_get_dmac(const urma_context_t *ctx, const urma_net_addr_t *net_addr, uint8_t *mac)
{
    return 0;
}
