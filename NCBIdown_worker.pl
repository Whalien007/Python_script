#!/usr/bin/perl
use strict;
use warnings;

# Add External perl module lib path
use FindBin qw( $RealBin );
use lib ( $RealBin =~ m#^(.*/NCBIdown)/.*$#i ? "$1/lib" : '/usr/local/NCBIdown/lib' );

use NCBIdownConfig;
use Data::Dumper;
use Text::TabularDisplay;
use IO::File;
use File::Basename;

# subs
sub update_job_status_to_local;
sub run_stage;


### job status in DB
# New                       新添加未开始的，所有新添加到数据库的文件记录，都标记为该状态
# Init                      Worker 初始化
# Failed_Init               Worker 初始化失败
# Downloading               Stage 1 ，下载
# Failed_stage_1            Stage 1 错误，下载失败
# Converting                Stage 2 ，转换
# Failed_stage_2            Stage 2 错误，转换失败
# Failed_stage_2_TO         Stage 2 错误，转换失败，TimeOut 
# Copying                   Stage 3 ，复制
# Failed_stage_3            Stage 3 错误，复制失败
# Failed_NCBI_Record_Error  NCBI 网站记录错误，单端的记录为双端的，或者反之
# OK                        正确完成，为兼容任务系统，暂时改为 success 


# About Stage 2 Converting TimeOut：
# TimeOut_time(s) = Size_of_SRA * 7 / 8(MB)

### Stage status
# Running    Sub job is Running
# Failed     Sub job Failed
# Done       Sub job Done



# Usage: NCBIdown_worker.pl $pkid $file_type $file_path
# $pkid - Primary key ID
my ( $pkid, $file_type, $file_path ) = @ARGV;
my $file_sra_name = "$file_type.sra";
( my $dir_file_type = $file_path ) =~ s#^/share/bioCloud/cloud/rawdata/(.*)/$file_type.*$#$1#;
my $dir_tmp_save = "$DownloadCtl_info->{Dir_Tmp_Save}/$dir_file_type";
my $dir_real_save = "$DownloadCtl_info->{Dir_Save}/$dir_file_type";
my $job = {
    id             => $pkid,
    file_type      => $file_type,
    pid            => $$,
    start_time     => time_now(),
    start_unixtime => time_now('%s'),
    status         => 'Init',
    stage          => [ "$pkid $file_type $file_path" ],
};
    


#### start job

## Stage 1: Downloading
# NCBI 下载网站有两个，有时会有个别无法访问的情况，此时就需要切换
# ftp-trace.ncbi.nih.gov
# ftp-trace.ncbi.nlm.nih.gov
my $ncbi_download_url = 'anonftp@ftp-trace.ncbi.nlm.nih.gov:/sra/sra-instant/reads/ByRun/sra/'
                      . substr($file_type, 0, 3) . "/"
                      . substr($file_type, 0, 6) . "/"
                      . $file_type . "/"
                      . $file_sra_name;
my $cmd_stage_1 = <<"EOF";
/share/nas1/xugl/chenc/software/conda3/bin/python3.7  /share/nas1/xugl/chenc/software/conda3/ncbi/SRA_receiver.py -sra_name $file_type \\
    -argument_l $JobCtl_info->{max_down_rate_per} -tmp_dir $dir_tmp_save/
EOF
run_stage( 1, 'Downloading', $cmd_stage_1, 3 );
$job->{stage}[1]{File_sra} = "$dir_tmp_save/$file_type.sra";


## Stage 2: Converting
my $cmd_stage_2 = <<"EOF";
/share/nas2/genome/biosoft/perl/current/bin/perl  \\
    /share/nas2/genome/biosoft/NCBIdown_ConvertSRATool/sra_2_fq_and_QC_for_single_file/sra_2_fq_and_QC.pl  \\
        -sra_file $dir_tmp_save/$file_sra_name  \\
        -od       $dir_tmp_save/
EOF
    
run_stage( 2, 'Converting', $cmd_stage_2 );

my @fastq_num = glob("$dir_tmp_save/*.fastq");
if(@fastq_num == 1){
    my $fastq = $fastq_num[0];
    $fastq = basename($fastq);
    chdir $dir_tmp_save;
    `rename _1.fastq .fastq $fastq`;
}

## Stage 2.5: 检查单端、双端正确性，存在 NCBI 记录错误，双端的被记录为单端
my $file_check_path = "$dir_tmp_save/" . basename($file_path);
my $NCBI_Record_OK  = -e $file_check_path ? 1 : 0;

## Stage 3: Copying
my $cmd_stage_3 = <<"EOF";
mkdir -p $dir_real_save &&
/bin/cp -f $dir_tmp_save/${file_type}*.fastq       \\
           $dir_real_save/                         &&
rm -f $dir_tmp_save/${file_type}*.fastq $dir_tmp_save/$file_sra_name
EOF

run_stage( 3, 'Copying', $cmd_stage_3 );


## At last, update status info
$job->{status}       = $NCBI_Record_OK ? 'success' : 'Failed_NCBI_Record_Error';
$job->{end_time}     = $job->{stage}[3]{end_time};
$job->{end_unixtime} = $job->{stage}[3]{end_unixtime};
log_msg( "File $pkid $file_type, ALL Done!" );
update_job_status_to_local();
exit 0;










sub update_job_status_to_local {
    # update jobs status in local file
    my $File_job_status = "$JobCtl_info->{Dir_jobs}/$pkid";
    my $FH_job_status = IO::File->new(" > $File_job_status ");
    
    $FH_job_status->print( Dumper $job );
    $FH_job_status->close();
}


# Usage: run_stage( $stage_number, $sub_job, $cmd );
sub run_stage {
    my ( $stage_number, $stage_name, $cmd, $retry_time ) = @_;
    ! defined $retry_time  and $retry_time = 1;

    # set jobs status
    $job->{status} = $stage_name;

    # make job stage struct
    my $stage = {
        stage          => "Stage $stage_number $stage_name",
        cmd            => $cmd,
        status         => 'Running',
        start_time     => time_now(),
        start_unixtime => time_now('%s'),
    };
    $job->{stage}[$stage_number] = $stage;

    # start now
    # output log and update job status
    log_msg("File $pkid $file_type, Stage $stage_number $stage_name Start! ");
    update_job_status_to_local();

    # run cmd
    my $lrunr = lrun( $stage->{cmd} );
    # To avoid stage 1 ascp download session timeout
    my $retry_interval = 900;
    while ( $lrunr->{code} != 0 && --$retry_time ) {
        $lrunr = lrun( $stage->{cmd} );
        $lrunr->{code} == 0  and last;
        sleep $retry_interval;
        $retry_interval += $retry_interval;
    }

    # save cmd execute result
    $stage->{end_time}     = time_now();
    $stage->{end_unixtime} = time_now('%s');
    $stage->{code}         = $lrunr->{code};
    $stage->{stdout}       = $lrunr->{stdout};
    $stage->{stderr}       = $lrunr->{stderr};

    # check cmd exit code, update stage status
    if ( $stage->{code} == 0 ) {
        $stage->{status} = 'Done';
        update_job_status_to_local();        
        
        log_msg("File $pkid $file_type, Stage $stage_number $stage_name OK! $stage->{stdout} ");
    } else {
        # set stage status
        $stage->{status} = 'Failed!';
        
        # set jobs status
        $job->{status}       = "Failed_stage_${stage_number}_$stage_name";
        $job->{end_time}     = $stage->{end_time};
        $job->{end_unixtime} = $stage->{end_unixtime};

        # update jobs status, both local file and DB
        update_job_status_to_local();
        
        log_msg("File $pkid $file_type, Stage $stage_number $stage_name Failed! code: $stage->{code}, $stage->{stderr} ");
        send_mail( "NCBIdown File $pkid $file_type Failed!", "NCBIdown File $pkid $file_type Failed!\n\nStage $stage_number $stage_name Failed! code: $stage->{code}\n\n  STDOUT: $stage->{stdout} \n\n  STDERR: $stage->{stderr} " );
        exit $stage_number
    }
}

