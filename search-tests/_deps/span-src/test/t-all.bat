@echo off & setlocal enableextensions enabledelayedexpansion
::
:: t-all.bat - perform tests with varying contract violation response, features and C++ standard (MSVC).
::

set log=%0.log
echo. > %log%

set /a compiler_version=0
call :CompilerVersion compiler_version

set spanProgram=span-main.t.exe
set spanSources=span-main.t.cpp span.t.cpp

set cvResponses=^
    span_CONFIG_CONTRACT_VIOLATION_THROWS ^
    span_CONFIG_CONTRACT_VIOLATION_TERMINATES

set spanFeatures=^
    "span_FEATURE_BYTE_SPAN=1" ^
    "span_FEATURE_MAKE_SPAN_TO_STD=99" ^
    "span_FEATURE_WITH_CONTAINER_TO_STD=99" ^
    "span_FEATURE_CONSTRUCTION_FROM_STDARRAY_ELEMENT_TYPE=1" ^
    "span_FEATURE_MEMBER_AT=2" ^
    "span_FEATURE_MEMBER_BACK_FRONT=1" ^
    "span_FEATURE_MEMBER_SWAP=1" ^
    "span_FEATURE_NON_MEMBER_FIRST_LAST_SUB=1" ^
    "span_FEATURE_SAME=1"

set cppStandards=^
    c++14 ^
    c++17 ^
    c++latest

set msvc_defines=^
    -D_CRT_SECURE_NO_WARNINGS ^
    -D_SCL_SECURE_NO_WARNINGS

set CppCoreCheckInclude=%VCINSTALLDIR%\Auxiliary\VS\include

call :ForAllCombinations
endlocal & goto :EOF

:: subroutines

:ForAllCombinations
:ForContractViolationResponse
for %%r in ( %cvResponses% ) do (
    call :ForSpanFeature -D%%r
)
goto :EOF

:ForSpanFeature  contractViolationResponse
for %%i in ( %spanFeatures% ) do (
    call :ForCppStd %* "-D%%i"
)
goto :EOF

:ForCppStd  contractViolationResponse spanFeature
if %compiler_version% LSS 14 (
    call :CompileLog %*
) else (
for %%s in ( %cppStandards% ) do (
    call :CompileLog %* -std:%%s
))
goto :EOF

:CompileLog  contractViolationResponse spanFeature [CppStd]
echo VC%compiler_version%: %*
call :Compile %* >> %log%  2>&1
if errorlevel 1 (
    less %log% & goto :EOF
) else (
    echo.%* | findstr /C:"THROWS" 1>nul
    if not errorlevel 1 ( %spanProgram% )
)
goto :EOF

:Compile  contractViolationResponse spanFeature [CppStd]
::call t.bat %*
set args=%*
set compile=cl -EHsc -I../include -I"%CppCoreCheckInclude%" %args% %msvc_defines% %spanSources%
echo %compile% && %compile%
goto :EOF

:CompilerVersion  version
@echo off & setlocal enableextensions
set tmpprogram=_getcompilerversion.tmp
set tmpsource=%tmpprogram%.c

echo #include ^<stdio.h^>                   >%tmpsource%
echo int main(){printf("%%d\n",_MSC_VER);} >>%tmpsource%

cl /nologo %tmpsource% >nul
for /f %%x in ('%tmpprogram%') do set version=%%x
del %tmpprogram%.* >nul
set offset=0
if %version% LSS 1900 set /a offset=1
set /a version="version / 10 - 10*(5 - offset)"
endlocal & set %1=%version%& goto :EOF
